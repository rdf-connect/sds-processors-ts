import { Bucketizer, PageFragmentation } from "./index";
import { Bucket, Record } from "../utils";
import { TREE } from "@treecg/types";
import { getLoggerFor } from "../utils/logUtil";

export default class PagedBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly pageSize: number;
    private count: number = 0;

    constructor(config: PageFragmentation, save?: string) {
        this.pageSize = config.pageSize;

        if (save) {
            this.count = JSON.parse(save);
        }
    }

    bucketize(
        _: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
    ): Bucket[] {
        const index = Math.floor(this.count / this.pageSize);
        this.count += 1;
        const currentbucket = getBucket("page-" + index, index == 0);

        if (this.count % this.pageSize == 1 && this.count > 1) {
            const oldBucket = getBucket("page-" + (index - 1), index - 1 == 0);
            oldBucket.immutable = true;
            oldBucket.addRelation(currentbucket, TREE.terms.Relation);

            this.logger.info(`Created new page bucket ${index}`);
        }

        return [currentbucket];
    }

    save() {
        return JSON.stringify(this.count);
    }
}
