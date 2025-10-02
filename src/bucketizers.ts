import { readFileSync, writeFileSync } from "fs";
import { Parser, Writer as N3Writer } from "n3";
import { Quad, Quad_Object, Quad_Subject, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { getLatestShape, getLatestStream, transformMetadata } from "./core";
import { LDES, PPLAN, PROV, RDF, SDS, XSD } from "@treecg/types";
import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
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
        let p = path;
        try {
            const url = new URL(path);
            p = url.pathname;
        } catch (_ex: unknown) {
            // this is fine, the path was already a file path, not a uri
        }
        writeFileSync(p, content, { encoding: "utf-8" });
    }
}

function addProcess(
    id: Term | undefined,
    store: RdfStore,
    strategies: { id: Term; quads: Quad[] }[],
): Term {
    const newId = df.blankNode();
    const time = new Date().toISOString();

    store.addQuad(df.quad(newId, RDF.terms.type, PPLAN.terms.Activity));
    store.addQuad(df.quad(newId, RDF.terms.type, LDES.terms.Bucketization));

    if (strategies.length === 1) {
        strategies[0].quads.forEach((q) => store.addQuad(q));
        store.addQuad(
            df.quad(newId, PROV.terms.used, <Quad_Object>strategies[0].id),
        );
    } else {
        let lastCollectionId: Term = RDF.terms.nil;
        for (const s of strategies) {
            s.quads.forEach((q) => store.addQuad(q));
            const collectionEntry = df.blankNode();

            store.addQuad(
                df.quad(collectionEntry, RDF.terms.rest, lastCollectionId),
            );
            store.addQuad(
                df.quad(collectionEntry, RDF.terms.first, <Quad_Object>s.id),
            );
            lastCollectionId = collectionEntry;
        }
        store.addQuad(df.quad(newId, PROV.terms.used, lastCollectionId));
    }

    store.addQuad(df.quad(newId, PROV.terms.startedAtTime, df.literal(time)));
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
    dataInput: Reader;
    metadataInput: Reader;
    dataOutput: Writer;
    metadataOutput: Writer;
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

function read_save(savePath?: string) {
    try {
        if (savePath) {
            return readFileSync(savePath, { encoding: "utf8" });
        }
    } catch (ex) {
        return;
    }
}

type Args = {
    channels: Channels;
    config: BucketizerConfig[];
    savePath: string | undefined;
    sourceStream: Term | undefined;
    resultingStream: Term;
    prefix: string;
};
export class Bucketizer extends Processor<Args> {
    orchestrator: BucketizerOrchestrator;
    extractor: Extractor;
    buckets: { [id: string]: Bucket } = {};

    async init(this: Args & this): Promise<void> {
        this.prefix = this.prefix ?? "root";
        const save = read_save(this.savePath);
        this.orchestrator = new BucketizerOrchestrator(this.config, save);
        this.extractor = new Extractor(
            new CBDShapeExtractor(),
            this.sourceStream,
        );

        handleExit(async () => {
            const state = this.orchestrator.save();
            await writeState(this.savePath, state);
        });
    }

    async transform(this: Args & this): Promise<void> {
        const promises: Promise<unknown>[] = [];
        promises.push(this.set_metadata());
        promises.push(this.extractShape());
        promises.push(this.transformData());

        await Promise.all(promises);
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }

    async set_metadata(this: Args & this) {
        const f = transformMetadata(
            this.resultingStream,
            this.sourceStream,
            "https://w3id.org/sds#Member",
            (x, y) =>
                // TODO: fix
                addProcess(
                    x,
                    y,
                    this.config.map((x) => x.quads),
                ),
        );

        this.logger.info("Accepting metadata");
        for await (const quads of this.channels.metadataInput.strings()) {
            this.logger.info("Got metadata input " + quads);
            await this.channels.metadataOutput.string(
                serializeQuads(await f(parseQuads(quads))),
            );
        }
        await this.channels.metadataOutput.close();
    }

    async extractShape(this: Args & this) {
        for await (const x of this.channels.metadataInput.strings()) {
            const quads = new Parser().parse(x);

            const store = RdfStore.createDefault();
            quads.forEach((q) => store.addQuad(q));

            const latest = this.sourceStream || (await getLatestStream(store));
            const latestShape = latest
                ? await getLatestShape(latest, store)
                : undefined;

            if (latestShape) {
                const rdfStore = RdfStore.createDefault();
                quads.forEach((x) => rdfStore.addQuad(x));
                const cbd_extract = new CBDShapeExtractor(rdfStore);

                this.extractor.extractor = cbd_extract;
                this.extractor.shape = latestShape;
            }
        }
    }

    async transformData(this: Args & this) {
        for await (const x of this.channels.dataInput.strings()) {
            const outputQuads: Quad[] = [];
            const quads = new Parser().parse(x);

            const records = await this.extractor.parse_records(quads);
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
                const record_buckets = this.orchestrator.bucketize(
                    record,
                    this.buckets,
                    requestedBuckets,
                    newMembers,
                    newRelations,
                    removeRelations,
                    this.prefix,
                );

                record_buckets.forEach((x) => requestedBuckets.add(x));

                // Write SDS Record for resulting stream
                outputQuads.push(
                    ...record_to_quads(
                        record,
                        this.resultingStream,
                        record_buckets.map((x) => this.buckets[x]),
                    ),
                );
            }

            // Write records for the new members.
            for (const [bucket, members] of newMembers) {
                // Check if bucket is not in emptyBuckets, otherwise we optimize by skipping this entry.
                if (this.buckets[bucket].empty) {
                    continue;
                }
                for (const member of members) {
                    outputQuads.push(
                        ...record_to_quads(
                            new Record(
                                { id: df.namedNode(member), quads: [] },
                                this.resultingStream,
                                this.buckets[bucket],
                            ),
                            this.resultingStream,
                            [this.buckets[bucket]],
                        ),
                    );
                }

                // Register the bucket as included in the output, so we do not have to write it again as requested bucket without relations
                requestedBuckets.add(bucket);
            }

            // Only write the requested buckets that are not included in the output yet
            for (const requestedBucket of requestedBuckets) {
                outputQuads.push(
                    ...bucket_to_quads(
                        this.buckets[requestedBucket],
                        this.resultingStream,
                    ),
                );
            }
            for (const { origin, relation } of newRelations) {
                outputQuads.push(...relationToQuads(origin, relation));
            }
            for (const { origin, relation } of removeRelations) {
                outputQuads.push(
                    ...relationToQuads(
                        origin,
                        relation,
                        this.resultingStream,
                        true,
                    ),
                );
            }

            await this.channels.dataOutput.string(
                new N3Writer().quadsToString(outputQuads),
            );
        }

        await writeState(this.savePath, this.orchestrator.save());
        // Close downstream channel
        await this.channels.dataOutput.close();
    }
}
