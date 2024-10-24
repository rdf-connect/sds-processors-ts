import { AddRelation, Bucketizer, PageFragmentation } from "./index";
import { Bucket, RdfThing, Record } from "../utils";
import { TREE, XSD } from "@treecg/types";
import { getLoggerFor } from "../utils/logUtil";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { DataFactory } from "n3";

const { literal, namedNode } = DataFactory;

export default class PagedBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly pageSize: number;
    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private count: number = 0;
    private lastMemberTimestamp: number = 0;

    constructor(config: PageFragmentation, save?: string) {
        this.pageSize = config.pageSize;

        if (config.path && config.pathQuads) {
            // Timestamp path is set, so we have an ordered paged bucketizer.
            this.path = config.path.mapAll((x) => ({
                value: x.id.value,
                literal: x.id,
            }));
            this.pathQuads = config.pathQuads;
        }

        if (save) {
            const parsed = JSON.parse(save);
            this.count = parsed.count;
            this.lastMemberTimestamp = parsed.lastMemberTimestamp;
        }
    }

    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
        addRelation: AddRelation,
    ): Bucket[] {
        const index = Math.floor(this.count / this.pageSize);
        this.count += 1;
        const currentbucket = getBucket("page-" + index, index == 0);

        let recordTimestamp: Date | undefined = undefined;
        if (this.path) {
            const values = this.path
                .execute(record.data)
                .filter(
                    (x, i, arr) =>
                        arr.findIndex((y) => x.value === y.value) == i,
                )
                .filter((value) => value.literal);

            if (values.length !== 1) {
                this.logger.error(
                    `Expected exactly one timestamp value, got ${values.length}. Ignoring record '${record.data.id.value}'.`,
                );
                return [];
            }

            recordTimestamp = new Date(values[0].literal!.value);

            // Check if the record is out of order.
            if (recordTimestamp.getTime() < this.lastMemberTimestamp) {
                this.logger.error(
                    `Record timestamp is before the last record timestamp. Are your records out of order? Ignoring record '${record.data.id.value}'.`,
                );
                return [];
            }
            this.lastMemberTimestamp = recordTimestamp.getTime();
        }

        if (this.count % this.pageSize == 1 && this.count > 1) {
            const oldBucket = getBucket("page-" + (index - 1), index - 1 == 0);
            oldBucket.immutable = true;
            if (recordTimestamp) {
                // Ordered paged bucketizer, add a ≥ relation to the previous bucket.
                addRelation(
                    oldBucket,
                    currentbucket,
                    TREE.terms.GreaterThanOrEqualToRelation,
                    literal(
                        recordTimestamp.toISOString(),
                        namedNode(XSD.dateTime),
                    ),
                    this.pathQuads,
                );
            } else {
                addRelation(oldBucket, currentbucket, TREE.terms.Relation);
            }
            this.logger.info(`Created new page bucket ${index}`);
        }

        return [currentbucket];
    }

    save() {
        return JSON.stringify({
            count: this.count,
            lastMemberTimestamp: this.lastMemberTimestamp,
        });
    }
}
