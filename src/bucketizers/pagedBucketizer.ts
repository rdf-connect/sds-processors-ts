import { AddRelation, Bucketizer, PageFragmentation } from "./index";
import { Bucket, Record } from "../utils";
import { TREE } from "@treecg/types";

export default class PagedBucketizer implements Bucketizer {
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
        addRelation: AddRelation,
    ): Bucket[] {
        const index = Math.floor(this.count / this.pageSize);
        this.count += 1;
        const currentbucket = getBucket("page-" + index, index == 0);

        if (this.count % this.pageSize == 1 && this.count > 1) {
            const oldBucket = getBucket("page-" + (index - 1), index - 1 == 0);
            oldBucket.immutable = true;
            addRelation(oldBucket, currentbucket, TREE.terms.Relation);
        }

        return [currentbucket];
    }

    save() {
        return JSON.stringify(this.count);
    }
}
