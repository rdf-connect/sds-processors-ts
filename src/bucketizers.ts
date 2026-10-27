import { readFileSync, writeFileSync } from "fs";
import { Parser, Writer as N3Writer } from "n3";
import { Quad, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
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

type Channels = {
    dataInput: Reader;
    dataOutput: Writer;
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
    prefix: string;
};
export class Bucketizer extends Processor<Args> {
    orchestrator: BucketizerOrchestrator;
    extractor: Extractor;
    buckets: { [id: string]: Bucket } = {};
    recordCount: number = 0;
    // The stream is the channel the records are written to.
    resultingStream: Term;

    async init(this: Args & this): Promise<void> {
        this.resultingStream = df.namedNode(this.channels.dataOutput.uri);
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
        await this.transformData();
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
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
