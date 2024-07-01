import { BasicLensM, Cont } from "rdf-lens";
import { Bucket, RdfThing, Record } from "../utils";
import { TREE } from "@treecg/types";
import { Bucketizer, PageFragmentation, SubjectFragmentation } from ".";
import { Term } from "@rdfjs/types";

export class PagedBucketizer implements Bucketizer {
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
        }

        return [currentbucket];
    }

    save() {
        return JSON.stringify(this.count);
    }
}

export class SubjectBucketizer implements Bucketizer {
    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private readonly namePath?: BasicLensM<Cont, Cont>;
    private readonly defaultName?: string;

    private seen: Set<string> = new Set();

    constructor(config: SubjectFragmentation, save?: string) {
        this.path = config.path.mapAll((x) => ({
            value: x.id.value,
            literal: x.id,
        }));
        this.pathQuads = config.pathQuads;
        this.namePath = config.namePath;
        this.defaultName = config.defaultName;

        if (save) {
            this.seen = new Set(JSON.parse(save));
        }
    }

    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
    ): Bucket[] {
        const values = this.path
            .execute(record.data)
            .filter(
                (x, i, arr) => arr.findIndex((y) => x.value === y.value) == i,
            );

        const out: Bucket[] = [];

        const root = getBucket("root", true);

        if (values.length === 0 && this.defaultName) {
            values.push({ value: this.defaultName });
        }

        if (values.length === 0) {
            console.error(
                "Didn't find bucket, and default name is not set, sadness :(",
            );
        }

        for (const value of values) {
            const name = value.literal
                ? this.namePath?.execute({
                      id: value.literal,
                      quads: record.data.quads,
                  })[0]?.id.value || value.value
                : value.value;

            const bucket = getBucket(name);

            if (!this.seen.has(bucket.id.value)) {
                this.seen.add(bucket.id.value);

                root.addRelation(
                    bucket,
                    TREE.terms.EqualToRelation,
                    value.literal,
                    value.literal ? this.pathQuads : undefined,
                );
            }

            out.push(bucket);
        }

        return out;
    }

    save() {
        return JSON.stringify([...this.seen.values()]);
    }
}
