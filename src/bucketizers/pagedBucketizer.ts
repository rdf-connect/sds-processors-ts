import {
    AddRelation,
    Bucketizer,
    PageFragmentation,
    RemoveRelation,
} from "./index";
import {
    Bucket,
    findAndValidateRecordTimestamp,
    RdfThing,
    Record,
} from "../utils";
import { TREE, XSD } from "@treecg/types";
import { getLoggerFor } from "../utils/logUtil";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { DataFactory } from "n3";

const { literal, namedNode } = DataFactory;

export default class PagedBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    protected readonly pageSize: number;
    protected readonly path: BasicLensM<
        Cont,
        { value: string; literal?: Term }
    >;
    protected readonly pathQuads: RdfThing;
    protected count: number = 0;
    protected lastMemberTimestamp: number = 0;

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
        removeRelation: RemoveRelation,
    ): Bucket[] {
        const index = Math.floor(this.count / this.pageSize);
        this.count += 1;
        const currentBucket = getBucket("page-" + index, index == 0);

        const recordTimestamp: Date | undefined | null =
            findAndValidateRecordTimestamp(
                record,
                this.path,
                this.lastMemberTimestamp,
                this.logger,
            );
        if (recordTimestamp === null) {
            return [];
        }
        if (recordTimestamp) {
            this.lastMemberTimestamp = recordTimestamp.getTime();
        }

        if (this.count % this.pageSize == 1 && this.count > 1) {
            const oldBucket = getBucket("page-" + (index - 1), index - 1 == 0);
            oldBucket.immutable = true;
            if (recordTimestamp) {
                // Ordered paged bucketizer, add a ≥ relation to the previous bucket.
                addRelation(
                    oldBucket,
                    currentBucket,
                    TREE.terms.GreaterThanOrEqualToRelation,
                    literal(
                        recordTimestamp.toISOString(),
                        namedNode(XSD.dateTime),
                    ),
                    this.pathQuads,
                );
            } else {
                addRelation(oldBucket, currentBucket, TREE.terms.Relation);
            }
            this.logger.info(`Created new page bucket ${index}`);
        }

        return [currentBucket];
    }

    save() {
        return JSON.stringify({
            count: this.count,
            lastMemberTimestamp: this.lastMemberTimestamp,
        });
    }
}
