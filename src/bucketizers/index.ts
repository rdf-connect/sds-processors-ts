import { Term } from "@rdfjs/types";
import { BasicLensM, Cont } from "rdf-lens";
import {
    Bucket,
    BucketRelation,
    getOrDefaultMap,
    RdfThing,
    Record,
} from "../utils";
import { TREE } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import PagedBucketizer from "./pagedBucketizer";
import SubjectBucketizer from "./subjectBucketizer";
import TimebasedBucketizer from "./timebasedBucketizer";

import { $INLINE_FILE } from "@ajuvercr/ts-transformer-inline-file";
import TimeBucketTreeBucketizer, {
    TimeBucketTreeConfig,
} from "./timeBucketTree";

export { TimeBucketTreeConfig } from "./timeBucketTree";

const df = new DataFactory();
export const SHAPES_TEXT = $INLINE_FILE("../../configs/bucketizer_configs.ttl");

export type BucketizerConfig = {
    type: Term;
    config:
        | SubjectFragmentation
        | PageFragmentation
        | TimebasedFragmentation
        | TimeBucketTreeConfig;
};

export type SubjectFragmentation = {
    path: BasicLensM<Cont, Cont>;
    pathQuads: Cont;
    defaultName?: string;
    namePath?: BasicLensM<Cont, Cont>;
};

export type PageFragmentation = {
    pageSize: number;
};

export type TimebasedFragmentation = {
    path: BasicLensM<Cont, Cont>;
    pathQuads: Cont;
    maxSize: number;
    k: number;
    minBucketSpan: number;
};

export type AddRelation = (
    origin: Bucket,
    target: Bucket,
    type: Term,
    value?: Term,
    path?: RdfThing,
) => void;

export interface Bucketizer {
    bucketize(
        sdsMember: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
        addRelation: AddRelation,
    ): Bucket[];

    save(): string;
}

type Save = { [key: string]: { bucketizer?: Bucketizer; save?: string } };

function createBucketizer(config: BucketizerConfig, save?: string): Bucketizer {
    switch (config.type.value) {
        case TREE.custom("SubjectFragmentation"):
            return new SubjectBucketizer(
                <SubjectFragmentation>config.config,
                save,
            );
        case TREE.custom("PageFragmentation"):
            return new PagedBucketizer(<PageFragmentation>config.config, save);
        case TREE.custom("TimebasedFragmentation"):
            return new TimebasedBucketizer(
                <TimebasedFragmentation>config.config,
                save,
            );
        case TREE.custom("TimeBucketFragmentation"):
            return new TimeBucketTreeBucketizer(
                <TimeBucketTreeConfig>config.config,
                save,
            );
    }
    throw "Unknown bucketizer " + config.type.value;
}

export class BucketizerOrchestrator {
    private readonly configs: BucketizerConfig[];

    private saves: Save = {};

    constructor(configs: BucketizerConfig[], save?: string) {
        this.configs = configs;

        if (save) {
            this.saves = JSON.parse(save);
        }
    }

    bucketize(
        record: Record,
        buckets: { [id: string]: Bucket },
        requestedBuckets: Set<string>,
        newMembers: Map<string, Set<string>>,
        newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[],
        prefix: string = "",
    ): string[] {
        let queue = [prefix];

        const addRelation = (
            origin: Bucket,
            target: Bucket,
            type: Term,
            value?: Term,
            path?: RdfThing,
        ) => {
            const relation = {
                type,
                value,
                path,
                target: target.id,
            };
            const newRel = {
                origin,
                relation,
            };
            newRelations.push(newRel);

            origin.links.push(relation);
            target.parent = origin;
        };

        for (let i = 0; i < this.configs.length; i++) {
            const todo = queue.slice();
            queue = [];

            for (const prefix of todo) {
                const bucketizer = this.getBucketizer(i, prefix);

                const getBucket = (value: string, root?: boolean) => {
                    const encodedValue = value
                        .split("/")
                        .map((x) => encodeURIComponent(decodeURIComponent(x)))
                        .join("/");
                    const key = value.endsWith("/")
                        ? encodedValue
                        : encodedValue + "/";
                    // const key = value;
                    // If the requested bucket is the root, it actually is the previous bucket

                    // avoid double slashes and leading slashes
                    const next =
                        prefix.endsWith("/") ||
                        prefix == "" ||
                        key.startsWith("/")
                            ? prefix + key
                            : prefix + "/" + key;
                    const id = root ? prefix : next;
                    if (!buckets[id]) {
                        buckets[id] = new Bucket(df.namedNode(id), [], false);

                        buckets[id].addMember = (memberId: string) => {
                            getOrDefaultMap(
                                newMembers,
                                id,
                                new Set<string>(),
                            ).add(memberId);
                        };
                    }

                    // This bucket is requested, please remember
                    requestedBuckets.add(id);

                    return buckets[id];
                };

                const foundBucket = bucketizer.bucketize(
                    record,
                    getBucket,
                    addRelation,
                );

                for (const bucket of foundBucket) {
                    queue.push(bucket.id.value);
                }
            }
        }

        if (buckets[prefix]) {
            buckets[prefix].root = true;
        }

        return queue;
    }

    save(): string {
        for (const key of Object.keys(this.saves)) {
            this.saves[key].save = this.saves[key].bucketizer?.save();
            delete this.saves[key].bucketizer;
        }
        return JSON.stringify(this.saves);
    }

    private getBucketizer(index: number, key: string): Bucketizer {
        if (!this.saves[key]) {
            this.saves[key] = {};
        }

        const save = this.saves[key];

        if (!save.bucketizer) {
            save.bucketizer = createBucketizer(this.configs[index], save.save);
        }

        return save.bucketizer!;
    }
}
