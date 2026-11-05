import type {
    Quad,
    Quad_Object,
    Quad_Predicate,
    Quad_Subject,
    Term,
} from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { Parser } from "n3";
import { LDES, PPLAN, PROV, RDF, RDFS, SDS, SHACL, TREE } from "@treecg/types";

const df = new DataFactory();

const RDFC_NS = "https://w3id.org/rdf-connect#";
const rdfc = (name: string) => df.namedNode(RDFC_NS + name);
const DCAT_DATASET = df.namedNode("http://www.w3.org/ns/dcat#Dataset");
const FIRST = df.namedNode(RDF.first);
const REST = df.namedNode(RDF.rest);
const NIL = df.namedNode(RDF.nil);
const SH_CLASS = SHACL.terms.custom("class");

type Direction = "in" | "out";

/**
 * Describes how a processor type derives the stream on its output channel.
 * `sources` are the streams the processor read, so that they can be described too.
 */
type Describer = (
    pipeline: Pipeline,
    processor: Term,
    stream: Term,
) => { quads: Quad[]; sources: Term[] };

/**
 * The pipeline description, as a processor receives it with `rdfl:Context`,
 * read for the SDS streams that flow through it.
 *
 * A stream is identified by the channel it is first written to, and the
 * activity that derived it is the processor that wrote that channel, named by
 * its IRI in the pipeline description. Only the processor types in DESCRIBERS
 * start a stream; any other processor is assumed to forward the records it
 * reads, so a stream is found by walking back through such processors.
 */
export class Pipeline {
    readonly quads: Quad[];
    // Channels each processor reads and writes, and the processors that write each channel.
    private readonly inputs = new Map<string, Term[]>();
    private readonly outputs = new Map<string, Term[]>();
    private readonly writers = new Map<string, Term[]>();
    private readonly descriptions = new Map<string, Quad[]>();

    constructor(quads: Quad[]) {
        this.quads = quads;
        this.indexChannels();
    }

    /** The streams whose records arrive on this channel. */
    streamsOn(channel: Term | string, seen = new Set<string>()): Term[] {
        const id = typeof channel === "string" ? channel : channel.value;
        if (seen.has(id)) return [];
        seen.add(id);

        const out: Term[] = [];
        for (const processor of this.writers.get(id) || []) {
            if (this.describer(processor)) {
                out.push(df.namedNode(id));
            } else {
                for (const input of this.inputs.get(processor.value) || []) {
                    out.push(...this.streamsOn(input, seen));
                }
            }
        }
        return dedup(out);
    }

    /** The processor that started this stream, if the pipeline describes it. */
    origin(stream: Term): Term | undefined {
        return (this.writers.get(stream.value) || []).find((p) =>
            this.describer(p),
        );
    }

    /**
     * The description of a stream and of the streams it was derived from:
     * what it carries, its dataset, and the processor that generated it.
     * Empty when the pipeline does not describe the stream.
     */
    describe(stream: Term): Quad[] {
        const cached = this.descriptions.get(stream.value);
        if (cached) return cached;
        // Guards against a cycle while this stream is being described.
        this.descriptions.set(stream.value, []);

        const processor = this.origin(stream);
        if (!processor) return [];
        const { quads, sources } = this.describer(processor)!(
            this,
            processor,
            stream,
        );
        const out = dedupQuads([
            ...quads,
            ...sources.flatMap((source) => this.describe(source)),
        ]);
        this.descriptions.set(stream.value, out);
        return out;
    }

    /** Objects of a subject and predicate in the pipeline description. */
    objects(subject: Term, predicate: Term): Term[] {
        return this.quads
            .filter(
                (q) =>
                    q.subject.equals(subject) && q.predicate.equals(predicate),
            )
            .map((q) => q.object);
    }

    /** The Concise Bounded Description of a term in the pipeline description. */
    cbd(term: Term, seen = new Set<string>()): Quad[] {
        if (seen.has(term.value)) return [];
        seen.add(term.value);
        const out: Quad[] = [];
        for (const q of this.quads.filter((q) => q.subject.equals(term))) {
            out.push(q);
            if (q.object.termType === "BlankNode") {
                out.push(...this.cbd(q.object, seen));
            }
        }
        return out;
    }

    /** The members of an RDF list, or the term itself when it is not a list. */
    list(term: Term): Term[] {
        if (term.equals(NIL)) return [];
        const first = this.objects(term, FIRST);
        if (first.length === 0) return [term];
        const rest = this.objects(term, REST);
        return [first[0], ...(rest.length ? this.list(rest[0]) : [])];
    }

    inputsOf(processor: Term): Term[] {
        return this.inputs.get(processor.value) || [];
    }

    private describer(processor: Term): Describer | undefined {
        for (const ty of this.objects(processor, RDF.terms.type)) {
            const describer = DESCRIBERS[ty.value];
            if (describer) return describer;
        }
    }

    private indexChannels() {
        const channels = new Set(
            this.quads
                .filter(
                    (q) =>
                        q.predicate.equals(RDF.terms.type) &&
                        (q.object.equals(rdfc("Reader")) ||
                            q.object.equals(rdfc("Writer"))),
                )
                .map((q) => q.subject.value),
        );

        // Whether a property refers to a channel that is read or written,
        // following the sh:class of the property in the processor shapes.
        const directions = new Map<string, Direction>();
        for (const q of this.quads.filter((q) =>
            q.predicate.equals(SH_CLASS),
        )) {
            const dir = q.object.equals(rdfc("Reader"))
                ? "in"
                : q.object.equals(rdfc("Writer"))
                  ? "out"
                  : undefined;
            if (!dir) continue;
            for (const path of this.objects(q.subject, SHACL.terms.path)) {
                if (!directions.has(path.value))
                    directions.set(path.value, dir);
            }
        }

        // Where each blank node is used, to climb from a nested configuration
        // (rdfc:channels [ ... ]) or a list up to the processor.
        const parents = new Map<string, Quad>();
        for (const q of this.quads) {
            if (
                q.object.termType === "BlankNode" &&
                !parents.has(q.object.value)
            ) {
                parents.set(q.object.value, q);
            }
        }

        for (const q of this.quads) {
            if (!channels.has(q.object.value)) continue;
            let predicate = q.predicate;
            let node: Term = q.subject;
            while (node.termType === "BlankNode") {
                const up = parents.get(node.value);
                if (!up) break;
                if (predicate.equals(FIRST) || predicate.equals(REST)) {
                    predicate = up.predicate;
                }
                node = up.subject;
            }
            const dir = directions.get(predicate.value);
            if (!dir || node.termType !== "NamedNode") continue;

            const index = dir === "in" ? this.inputs : this.outputs;
            push(index, node.value, q.object);
            if (dir === "out") push(this.writers, q.object.value, node);
        }
    }
}

function push(map: Map<string, Term[]>, key: string, value: Term) {
    const values = map.get(key) || [];
    if (!values.some((v) => v.equals(value))) values.push(value);
    map.set(key, values);
}

function dedup(terms: Term[]): Term[] {
    return terms.filter((t, i) => terms.findIndex((o) => o.equals(t)) === i);
}

export function dedupQuads(quads: Quad[]): Quad[] {
    const seen = new Set<string>();
    return quads.filter((q) => {
        const key = [q.subject, q.predicate, q.object, q.graph]
            .map((t) => t.termType + ":" + t.value)
            .join(" ");
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

function quad(s: Term, p: Term, o: Term): Quad {
    return df.quad(<Quad_Subject>s, <Quad_Predicate>p, <Quad_Object>o);
}

/** The stream, the processor as the activity that generated it, and what it used. */
function derivation(
    pipeline: Pipeline,
    processor: Term,
    stream: Term,
    used: Term[],
): Quad[] {
    return [
        quad(stream, RDF.terms.type, SDS.terms.Stream),
        quad(stream, PROV.terms.wasGeneratedBy, processor),
        quad(processor, RDF.terms.type, PPLAN.terms.Activity),
        ...pipeline
            .objects(processor, RDF.terms.type)
            .map((ty) => quad(processor, RDF.terms.type, ty)),
        ...used.map((u) => quad(processor, PROV.terms.used, u)),
    ];
}

/** A derived stream carries the same members, in the same dataset, as its source. */
function inherit(pipeline: Pipeline, stream: Term, source: Term): Quad[] {
    const description = pipeline.describe(source);
    return [SDS.terms.carries, SDS.terms.dataset].flatMap((p) =>
        description
            .filter((q) => q.subject.equals(source) && q.predicate.equals(p))
            .map((q) => quad(stream, p, q.object)),
    );
}

/** The streams a processor read, optionally restricted to one configured stream. */
function sourcesOf(pipeline: Pipeline, processor: Term, filter: Term): Term[] {
    const sources = pipeline
        .inputsOf(processor)
        .flatMap((input) => pipeline.streamsOn(input));
    const only = pipeline.objects(processor, filter);
    return only.length
        ? sources.filter((s) => only.some((o) => o.equals(s)))
        : sources;
}

let shapeCount = 0;

const describeSdsify: Describer = (pipeline, processor, stream) => {
    const quads = derivation(
        pipeline,
        processor,
        stream,
        pipeline.inputsOf(processor),
    );
    const config = pipeline.objects(processor, rdfc("metadataConfig"))[0];
    const value = (name: string) =>
        config ? pipeline.objects(config, rdfc(name))[0] : undefined;

    const description = value("description");
    if (description)
        quads.push(quad(processor, RDFS.terms.comment, description));

    // The shape: named in the configuration with its graph, or the main node
    // shape of the shape that Sdsify extracts members with.
    let shapeId = value("shapeIri");
    const shapeText =
        value("shape")?.value ||
        pipeline.objects(processor, rdfc("shape"))[0]?.value;
    if (shapeText) {
        // A prefix of its own, so that the shape's blank nodes stay apart from others.
        const shapeQuads = new Parser({
            blankNodePrefix: `shape${shapeCount++}_`,
        }).parse(shapeText);
        quads.push(...shapeQuads);
        shapeId = shapeId || mainNodeShape(shapeQuads);
    }

    const member = df.blankNode();
    quads.push(
        quad(stream, SDS.terms.carries, member),
        quad(member, RDF.terms.type, SDS.terms.Member),
    );
    if (shapeId) quads.push(quad(member, SDS.terms.shape, shapeId));

    // The dataset: described in the pipeline, or made up from the configuration.
    const datasetId = value("dataset") || df.blankNode();
    quads.push(quad(stream, SDS.terms.dataset, datasetId));
    if (datasetId.termType === "NamedNode")
        quads.push(...pipeline.cbd(datasetId));
    if (!pipeline.objects(datasetId, RDF.terms.type).length) {
        quads.push(quad(datasetId, RDF.terms.type, DCAT_DATASET));
    }
    const timestampPath = value("timestampPath");
    if (timestampPath) {
        quads.push(quad(datasetId, LDES.terms.timestampPath, timestampPath));
    }
    const versionOfPath = value("versionOfPath");
    if (versionOfPath) {
        quads.push(quad(datasetId, LDES.terms.versionOfPath, versionOfPath));
    }
    if (shapeId) quads.push(quad(datasetId, TREE.terms.shape, shapeId));

    return { quads, sources: [] };
};

const describeBucketize: Describer = (pipeline, processor, stream) => {
    const sources = sourcesOf(pipeline, processor, rdfc("inputStreamId"));

    // The strategies, in the order of the configuration, as the pipeline states them.
    const configured = pipeline.objects(processor, rdfc("bucketizeStrategy"));
    const strategies = configured.flatMap((c) => pipeline.list(c));
    const configuration = [
        ...configured.map((c) => quad(processor, rdfc("bucketizeStrategy"), c)),
        ...configured.flatMap((c) =>
            c.termType === "BlankNode" ? pipeline.cbd(c) : [],
        ),
        ...strategies.flatMap((s) => pipeline.cbd(s)),
    ];

    return {
        quads: [
            ...derivation(pipeline, processor, stream, [
                ...sources,
                ...strategies,
            ]),
            quad(processor, RDF.terms.type, LDES.terms.Bucketization),
            ...configuration,
            ...sources.flatMap((source) => inherit(pipeline, stream, source)),
        ],
        sources,
    };
};

const describeLdesify: Describer = (pipeline, processor, stream) => {
    const sources = sourcesOf(pipeline, processor, rdfc("sourceStream"));
    return {
        quads: [
            ...derivation(pipeline, processor, stream, sources),
            // Versioning changes the members, so only the dataset carries over.
            ...sources.flatMap((source) =>
                inherit(pipeline, stream, source).filter((q) =>
                    q.predicate.equals(SDS.terms.dataset),
                ),
            ),
        ],
        sources,
    };
};

const DESCRIBERS: { [type: string]: Describer } = {
    [RDFC_NS + "Sdsify"]: describeSdsify,
    [RDFC_NS + "Bucketize"]: describeBucketize,
    [RDFC_NS + "LdesifySDS"]: describeLdesify,
};

/** The node shape that no other shape refers to. */
function mainNodeShape(quads: Quad[]): Term | undefined {
    const shapes = quads
        .filter(
            (q) =>
                q.predicate.equals(RDF.terms.type) &&
                q.object.equals(SHACL.terms.NodeShape),
        )
        .map((q) => q.subject);
    return shapes.find((s) => !quads.some((q) => q.object.equals(s)));
}
