import { readFileSync, writeFileSync } from "fs";
import { Parser, Writer as N3Writer } from "n3";
import { Quad, Quad_Object, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { transformMetadata } from "./core";
import { LDES, PPLAN, PROV, RDF } from "@treecg/types";
import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { BucketizerConfig, BucketizerOrchestrator } from "./bucketizers/index";
import { Bucket, BucketRelation, Record } from "./utils/index";
import {
    bucket_to_quads,
    Extractor,
    record_to_quads,
    relationToQuads,
} from "./utils/description";
import { cleanUpHandlers, handleExit } from "./exitHandler";
import { RdfStore } from "rdf-stores";
import { toPath } from "@rdfc/utils-js";

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
    strategies: { id: Term; quads: Quad[] }[],
): Term {
    const newId = df.blankNode();
    const time = new Date().toISOString();

    store.addQuad(df.quad(newId, RDF.terms.type, PPLAN.terms.Activity));
    store.addQuad(df.quad(newId, RDF.terms.type, LDES.terms.Bucketization));

    const valueTerm = PROV.terms.custom("value");
    const revisionTerm = PROV.terms.custom("wasRevisionOf");
    if (strategies.length === 1) {
        strategies[0].quads.forEach((q) => store.addQuad(q));
        store.addQuad(
            df.quad(newId, PROV.terms.used, <Quad_Object>strategies[0].id),
        );
    } else {
        // Push the first strategy to the end
        let lastCollectionId = df.blankNode();
        store.addQuad(
            df.quad(lastCollectionId, valueTerm, <Quad_Object>strategies[0].id),
        );
        strategies[0].quads.forEach((q) => store.addQuad(q));

        // Each strategy is prov:value on the config and a derivation from the previous one
        for (const s of strategies.slice(1)) {
            const collectionEntry = df.blankNode();

            store.addQuad(
                df.quad(
                    collectionEntry,
                    revisionTerm,
                    <Quad_Object>lastCollectionId,
                ),
            );
            store.addQuad(
                df.quad(collectionEntry, valueTerm, <Quad_Object>s.id),
            );
            lastCollectionId = collectionEntry;
        }

        store.addQuad(
            df.quad(newId, PROV.terms.used, <Quad_Object>lastCollectionId),
        );
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

function read_save(savePath?: string) {
    try {
        if (savePath) {
            return readFileSync(savePath, { encoding: "utf8" });
        }
    } catch {
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
    recordCount: number = 0;

    async init(this: Args & this): Promise<void> {
        if (this.savePath) this.savePath = toPath(this.savePath);
        const save = read_save(this.savePath);
        this.orchestrator = new BucketizerOrchestrator(this.config, save);
        this.extractor = new Extractor(this.sourceStream);

        handleExit(async () => {
            const state = this.orchestrator.save();
            await writeState(this.savePath, state);
        });
        this.logger.info("[init] Bucketizer processor initialized");
    }

    async transform(this: Args & this): Promise<void> {
        const promises: Promise<unknown>[] = [];
        promises.push(this.set_metadata());
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
                addProcess(
                    x,
                    y,
                    this.config.map((x) => x.quads),
                ),
        );

        this.logger.info("[set_metadata] Accepting metadata");
        for await (const quads of this.channels.metadataInput.strings()) {
            this.logger.verbose("[set_metadata] Got metadata input:\n" + quads);
            await this.channels.metadataOutput.string(
                serializeQuads(await f(parseQuads(quads))),
            );
        }
        await this.channels.metadataOutput.close();
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
                this.logger.debug(
                    `[transformData] Processing data record <${record.data.id.value}>`,
                );
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
                this.recordCount++;
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
                this.logger.debug(
                    "[transformData] Adding new relation from Bucket '" +
                        origin.id.value +
                        "' to Bucket '" +
                        relation.target.value +
                        "'of type <" +
                        relation.type.value +
                        ">, over property <" +
                        relation.path?.id.value +
                        "> and value " +
                        relation.value?.value,
                );
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
                this.logger.debug(
                    "[transformData] Removing relation from Bucket '" +
                        origin.id.value +
                        "' to Bucket '" +
                        relation.target.value +
                        "'of type <" +
                        relation.type.value +
                        ">, over property <" +
                        relation.path?.id.value +
                        "> and value " +
                        relation.value?.value,
                );
            }

            await this.channels.dataOutput.string(
                new N3Writer().quadsToString(outputQuads),
            );
        }

        this.logger.info(
            "[transformData] Bucketized " +
                this.recordCount +
                " records into " +
                Object.keys(this.buckets).length +
                " buckets",
        );
        await writeState(this.savePath, this.orchestrator.save());
        // Close downstream channel
        await this.channels.dataOutput.close();
        // Clean up exit handlers
        cleanUpHandlers();
    }
}
