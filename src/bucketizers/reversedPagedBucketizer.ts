import {
    AddRelation,
    Bucketizer,
    PageFragmentation,
    RemoveRelation,
} from "./index";
import { Bucket, findAndValidateRecordTimestamp, Record } from "../utils";
import { getLoggerFor } from "../utils/logUtil";
import PagedBucketizer from "./pagedBucketizer";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";

const { literal, namedNode } = DataFactory;

export default class ReversedPagedBucketizer
    extends PagedBucketizer
    implements Bucketizer
{
    protected readonly logger = getLoggerFor(this);

    constructor(config: PageFragmentation, save?: string) {
        super(config, save);
    }

    override bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ): Bucket[] {
        const index = Math.floor(this.count / this.pageSize);
        this.count++;
        const currentBucket = getBucket("page-" + index);

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

        if (this.count === 1) {
            // First record, first page-bucket, add relation from navigator bucket.
            const navigatorBucket = getBucket("", true);
            addRelation(navigatorBucket, currentBucket, TREE.terms.Relation);

            this.logger.info("Created first page and navigator buckets");
        } else if (this.count % this.pageSize === 1 && this.count > 1) {
            // New page-bucket, update relation from navigator bucket.
            const oldBucket = getBucket("page-" + (index - 1));
            const navigatorBucket = getBucket("", true);
            removeRelation(navigatorBucket, oldBucket, TREE.terms.Relation);
            addRelation(navigatorBucket, currentBucket, TREE.terms.Relation);

            // Add relation from new page-bucket to previous page-bucket.
            oldBucket.immutable = true;
            if (recordTimestamp) {
                // Ordered paged bucketizer, add a ≤ relation to the previous bucket.
                addRelation(
                    currentBucket,
                    oldBucket,
                    TREE.terms.LessThanOrEqualToRelation,
                    literal(
                        recordTimestamp.toISOString(),
                        namedNode(XSD.dateTime),
                    ),
                    this.pathQuads,
                );
            } else {
                addRelation(currentBucket, oldBucket, TREE.terms.Relation);
            }
            this.logger.info(`Created new page bucket ${index}`);
        }
        return [currentBucket];
    }

    override save(): string {
        return super.save();
    }
}
