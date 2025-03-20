import { readFileSync, writeFileSync } from "fs";
import { Parser, Writer as N3Writer } from "n3";
import { Quad, Quad_Object, Quad_Subject, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { getLatestShape, getLatestStream, transformMetadata } from "./core";
import { LDES, PPLAN, PROV, RDF, SDS, XSD } from "@treecg/types";
import type { Stream, Writer } from "@rdfc/js-runner";
import { BucketizerConfig, BucketizerOrchestrator } from "./bucketizers/index";
import { Bucket, BucketRelation, Extractor, Record } from "./utils/index";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { handleExit } from "./exitHandler";
import { RdfStore } from "rdf-stores";

const df = new DataFactory();

async function writeState(
    path: string | undefined,
    content: string,
): Promise<void> {
    if (path) {
        writeFileSync(path, content, { encoding: "utf-8" });
    }
}

function addProcess(
    id: Term | undefined,
    store: RdfStore,
    strategyId: Term,
    bucketizeConfig: Quad[],
): Term {
    const newId = df.blankNode();
    const time = new Date().toISOString();

    store.addQuad(df.quad(newId, RDF.terms.type, PPLAN.terms.Activity));
    store.addQuad(df.quad(newId, RDF.terms.type, LDES.terms.Bucketization));

    bucketizeConfig.forEach((q) => store.addQuad(q));

    store.addQuad(df.quad(newId, PROV.terms.startedAtTime, df.literal(time)));
    store.addQuad(df.quad(newId, PROV.terms.used, <Quad_Object>strategyId));
    if (id) {
        store.addQuad(df.quad(newId, PROV.terms.used, <Quad_Object>id));
    }

    return newId;
}

function parseQuads(quads: string | Quad[]): Quad[] {
    if (quads instanceof Array) return <Quad[]>quads;
    const parser = new Parser();
    return parser.parse(quads);
}

function serializeQuads(quads: Quad[]): string {
    const writer = new N3Writer();
    return writer.quadsToString(quads);
}

type Channels = {
    dataInput: Stream<string>;
    metadataInput: Stream<string>;
    dataOutput: Writer<string>;
    metadataOutput: Writer<string>;
};

type Config = {
    quads: { id: Term; quads: Quad[] };
    strategy: BucketizerConfig[];
};

export function record_to_quads(
    record: Record,
    resultingStream: Term,
    buckets: Bucket[],
): Quad[] {
    const id = df.blankNode();
    const out: Quad[] = [
        df.quad(
            id,
            SDS.terms.payload,
            <Quad_Object>record.data.id,
            SDS.terms.custom("DataDescription"),
        ),
        df.quad(
            id,
            SDS.terms.stream,
            <Quad_Object>resultingStream,
            SDS.terms.custom("DataDescription"),
        ),
        ...buckets
            .map((bucket) => bucket.id)
            .map((bucket) =>
                df.quad(
                    id,
                    SDS.terms.bucket,
                    <Quad_Object>bucket,
                    SDS.terms.custom("DataDescription"),
                ),
            ),
        ...(record.data.quads.length
            ? record.data.quads
            : [
                  df.quad(
                      id,
                      SDS.terms.custom("dataless"),
                      df.literal("true", XSD.terms.custom("boolean")),
                      SDS.terms.custom("DataDescription"),
                  ),
              ]),
    ];
    return out;
}

function bucket_to_quads(bucket: Bucket, stream?: Term): Quad[] {
    const out: Quad[] = [
        df.quad(
            <Quad_Subject>bucket.id,
            RDF.terms.type,
            SDS.terms.custom("Bucket"),
            SDS.terms.custom("DataDescription"),
        ),
    ];
    out.push(
        df.quad(
            <Quad_Subject>bucket.id,
            SDS.terms.custom("immutable"),
            df.literal((bucket.immutable || false) + ""),
            SDS.terms.custom("DataDescription"),
        ),
    );

    if (bucket.root) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.custom("isRoot"),
                df.literal("true"),
                SDS.terms.custom("DataDescription"),
            ),
        );
    }

    // Send empty triples for the bucket if it is set as true.
    if (bucket.empty) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.custom("empty"),
                df.literal("true"),
                SDS.terms.custom("DataDescription"),
            ),
        );
        bucket.empty = false;
    }

    if (stream) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.stream,
                <Quad_Object>stream,
                SDS.terms.custom("DataDescription"),
            ),
        );
    }

    return out;
}

function relationToQuads(
    bucket: Bucket,
    rel: BucketRelation,
    stream?: Term,
    remove?: boolean,
): Quad[] {
    const graph = remove
        ? SDS.terms.custom("RemoveDataDescription")
        : SDS.terms.custom("DataDescription");
    const out: Quad[] = [];
    const id = df.blankNode();
    out.push(
        df.quad(<Quad_Subject>bucket.id, SDS.terms.relation, id, graph),
        df.quad(
            id,
            RDF.terms.type,
            <Quad_Object>SDS.terms.custom("Relation"),
            graph,
        ),
        df.quad(id, SDS.terms.relationType, <Quad_Object>rel.type, graph),
        df.quad(id, SDS.terms.relationBucket, <Quad_Object>rel.target, graph),
    );

    if (rel.path) {
        out.push(
            df.quad(
                id,
                SDS.terms.relationPath,
                <Quad_Object>rel.path.id,
                graph,
            ),
            ...rel.path.quads.map((x) =>
                df.quad(x.subject, x.predicate, x.object, graph),
            ),
        );
    }

    if (rel.value) {
        out.push(
            df.quad(id, SDS.terms.relationValue, <Quad_Object>rel.value, graph),
        );
    }

    if (stream) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.stream,
                <Quad_Object>stream,
                graph,
            ),
        );
    }

    return out;
}

function set_metadata(
    channels: Channels,
    resultingStream: Term,
    sourceStream: Term | undefined,
    config: Config,
) {
    const f = transformMetadata(
        resultingStream,
        sourceStream,
        "https://w3id.org/sds#Member",
        (x, y) => addProcess(x, y, config.quads.id, config.quads.quads),
    );
    channels.metadataInput.data(async (quads) =>
        channels.metadataOutput.push(
            serializeQuads(await f(parseQuads(quads))),
        ),
    );

    channels.metadataInput.on("end", async () => {
        // Propagate the end event to the output metadata channel
        await channels.metadataOutput.end();
    });
}

function read_save(savePath?: string) {
    try {
        if (savePath) {
            return readFileSync(savePath, { encoding: "utf8" });
        }
    } catch (ex) {
        return;
    }
}

export async function bucketize(
    channels: Channels,
    config: Config,
    savePath: string | undefined,
    sourceStream: Term | undefined,
    resultingStream: Term,
    prefix = "root",
) {
    set_metadata(channels, resultingStream, sourceStream, config);
    const save = read_save(savePath);
    const orchestrator = new BucketizerOrchestrator(config.strategy, save);
    const extractor = new Extractor(new CBDShapeExtractor(), sourceStream);

    channels.metadataInput.data(async (x) => {
        const quads = new Parser().parse(x);

        const store = RdfStore.createDefault();
        quads.forEach((q) => store.addQuad(q));

        const latest = sourceStream || (await getLatestStream(store));
        const latestShape = latest
            ? await getLatestShape(latest, store)
            : undefined;

        if (latestShape) {
            const rdfStore = RdfStore.createDefault();
            quads.forEach((x) => rdfStore.addQuad(x));
            const cbd_extract = new CBDShapeExtractor(rdfStore);

            extractor.extractor = cbd_extract;
            extractor.shape = latestShape;
        }
    });

    handleExit(async () => {
        const state = orchestrator.save();
        await writeState(savePath, state);
    });

    const buckets: { [id: string]: Bucket } = {};
    channels.dataInput.data(async (x) => {
        const outputQuads: Quad[] = [];
        const quads = new Parser().parse(x);

        const records = await extractor.parse_records(quads);
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();

        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const removeRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];

        for (const record of records) {
            const record_buckets = orchestrator.bucketize(
                record,
                buckets,
                requestedBuckets,
                newMembers,
                newRelations,
                removeRelations,
                prefix,
            );

            record_buckets.forEach((x) => requestedBuckets.add(x));

            // Write SDS Record for resulting stream
            outputQuads.push(
                ...record_to_quads(
                    record,
                    resultingStream,
                    record_buckets.map((x) => buckets[x]),
                ),
            );
        }

        // Write records for the new members.
        for (const [bucket, members] of newMembers) {
            // Check if bucket is not in emptyBuckets, otherwise we optimize by skipping this entry.
            if (buckets[bucket].empty) {
                continue;
            }
            for (const member of members) {
                outputQuads.push(
                    ...record_to_quads(
                        new Record(
                            { id: df.namedNode(member), quads: [] },
                            resultingStream,
                            buckets[bucket],
                        ),
                        resultingStream,
                        [buckets[bucket]],
                    ),
                );
            }

            // Register the bucket as included in the output, so we do not have to write it again as requested bucket without relations
            requestedBuckets.add(bucket);
        }

        // Only write the requested buckets that are not included in the output yet
        for (const requestedBucket of requestedBuckets) {
            outputQuads.push(
                ...bucket_to_quads(buckets[requestedBucket], resultingStream),
            );
        }
        for (const { origin, relation } of newRelations) {
            outputQuads.push(...relationToQuads(origin, relation));
        }
        for (const { origin, relation } of removeRelations) {
            outputQuads.push(
                ...relationToQuads(origin, relation, resultingStream, true),
            );
        }

        // await prettyPrintQuads(outputQuads);

        await channels.dataOutput.push(
            new N3Writer().quadsToString(outputQuads),
        );
    });

    channels.dataInput.on("end", async () => {
        // Save the state of the orchestrator (if any)
        await writeState(savePath, orchestrator.save());
        // Close downstream channel
        await channels.dataOutput.end();
    });
}
