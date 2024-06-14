import { readFileSync, writeFileSync } from "fs";
import { DataFactory, Parser, Store } from "n3";
import * as N3 from "n3";
import {
    blankNode,
    getLatestShape,
    getLatestStream,
    literal,
    transformMetadata,
} from "./core";
import { LDES, PPLAN, PROV, RDF, SDS } from "@treecg/types";
import type { Stream, Writer } from "@rdfc/js-runner";
import { BucketizerConfig, BucketizerOrchestrator } from "./bucketizers/index";
import { Quad, Quad_Object, Term } from "rdf-js";
import { Bucket, Extractor, Record } from "./utils/index";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { Cleanup } from "./exitHandler";
import { RdfStore } from "rdf-stores";

const { quad } = DataFactory;

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
    store: Store,
    strategyId: Term,
    bucketizeConfig: Quad[],
): Term {
    const newId = store.createBlankNode();
    const time = new Date().toISOString();

    store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
    store.addQuad(newId, RDF.terms.type, LDES.terms.Bucketization);

    store.addQuads(bucketizeConfig);

    store.addQuad(newId, PROV.terms.startedAtTime, literal(time));
    store.addQuad(newId, PROV.terms.used, <Quad_Object>strategyId);
    if (id) {
        store.addQuad(newId, PROV.terms.used, <Quad_Object>id);
    }

    return newId;
}

function parseQuads(quads: string | Quad[]): Quad[] {
    if (quads instanceof Array) return <Quad[]>quads;
    const parser = new N3.Parser();
    return parser.parse(quads);
}

function serializeQuads(quads: Quad[]): string {
    const writer = new N3.Writer();
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

function record_to_quads(
    record: Record,
    resultingStream: Term,
    buckets: Bucket[],
): Quad[] {
    const id = blankNode();
    const out: Quad[] = [
        quad(
            id,
            SDS.terms.payload,
            <N3.Quad_Object>record.data.id,
            SDS.terms.custom("DataDescription"),
        ),
        quad(
            id,
            SDS.terms.stream,
            <N3.Quad_Object>resultingStream,
            SDS.terms.custom("DataDescription"),
        ),
        ...buckets
            .map((bucket) => bucket.id)
            .map((bucket) =>
                quad(
                    id,
                    SDS.terms.bucket,
                    <N3.Quad_Object>bucket,
                    SDS.terms.custom("DataDescription"),
                ),
            ),
        ...record.data.quads,
    ];
    return out;
}

function bucket_to_quads(bucket: Bucket): Quad[] {
    const out: Quad[] = [
        quad(
            <N3.Quad_Subject>bucket.id,
            RDF.terms.type,
            SDS.terms.custom("Bucket"),
            SDS.terms.custom("DataDescription"),
        ),
    ];
    out.push(
        quad(
            <N3.Quad_Subject>bucket.id,
            SDS.terms.custom("immutable"),
            literal((bucket.immutable || false) + ""),
            SDS.terms.custom("DataDescription"),
        ),
    );

    if (bucket.root) {
        out.push(
            quad(
                <N3.Quad_Subject>bucket.id,
                SDS.terms.custom("isRoot"),
                literal("true"),
                SDS.terms.custom("DataDescription"),
            ),
        );
    }

    for (const rel of bucket.links) {
        const id = blankNode();
        out.push(
            quad(
                <N3.Quad_Subject>bucket.id,
                SDS.terms.relation,
                id,
                SDS.terms.custom("DataDescription"),
            ),
            quad(
                id,
                SDS.terms.relationType,
                <N3.Quad_Object>rel.type,
                SDS.terms.custom("DataDescription"),
            ),
            quad(
                id,
                SDS.terms.relationBucket,
                <N3.Quad_Object>rel.target,
                SDS.terms.custom("DataDescription"),
            ),
        );

        if (rel.path) {
            out.push(
                quad(
                    id,
                    SDS.terms.relationPath,
                    <N3.Quad_Object>rel.path.id,
                    SDS.terms.custom("DataDescription"),
                ),
                ...rel.path.quads.map((x) =>
                    quad(
                        x.subject,
                        x.predicate,
                        x.object,
                        SDS.terms.custom("DataDescription"),
                    ),
                ),
            );
        }

        if (rel.value) {
            out.push(
                quad(
                    id,
                    SDS.terms.relationValue,
                    <N3.Quad_Object>rel.value,
                    SDS.terms.custom("DataDescription"),
                ),
            );
        }
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
        "sds:Member",
        (x, y) => addProcess(x, y, config.quads.id, config.quads.quads),
    );
    channels.metadataInput.data((quads) =>
        channels.metadataOutput.push(serializeQuads(f(parseQuads(quads)))),
    );
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
) {
    set_metadata(channels, resultingStream, sourceStream, config);
    const save = read_save(savePath);
    const orchestrator = new BucketizerOrchestrator(config.strategy, save);
    const extractor = new Extractor(new CBDShapeExtractor(), sourceStream);

    channels.metadataInput.data((x) => {
        const quads = new Parser().parse(x);

        const store = new Store();
        store.addQuads(quads);

        const latest = sourceStream || getLatestStream(store);
        const latestShape = latest ? getLatestShape(latest, store) : undefined;

        if (latestShape) {
            const rdfStore = RdfStore.createDefault();
            quads.forEach((x) => rdfStore.addQuad(x));
            const cbd_extract = new CBDShapeExtractor(rdfStore);

            extractor.extractor = cbd_extract;
            extractor.shape = latestShape;
        }
    });

    Cleanup(async () => {
        const state = orchestrator.save();
        await writeState(savePath, state);
    });

    const buckets: { [id: string]: Bucket } = {};
    channels.dataInput.data(async (x) => {
        const outputQuads: Quad[] = [];
        const quads = new Parser().parse(x);

        const records = await extractor.parse_records(quads);
        const relatedBuckets = new Set<string>();

        for (const record of records) {
            const record_buckets = orchestrator.bucketize(
                record,
                buckets,
                sourceStream?.value || "root",
            );
            outputQuads.push(
                ...record_to_quads(
                    record,
                    resultingStream,
                    record_buckets.map((x) => buckets[x]),
                ),
            );

            for (const b of record_buckets) {
                relatedBuckets.add(b);
                let parent = buckets[b].parent;
                while (parent) {
                    relatedBuckets.add(parent.id.value);
                    parent = parent.parent;
                }
            }
        }

        for (const relatedBucket of relatedBuckets.values()) {
            outputQuads.push(...bucket_to_quads(buckets[relatedBucket]));
        }

        await channels.dataOutput.push(
            new N3.Writer().quadsToString(outputQuads),
        );
    });
}