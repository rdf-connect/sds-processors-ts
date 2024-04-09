import { readFileSync } from "fs";
import * as path from "path";
import { Term } from "rdf-js";
import { BasicLensM, Cont } from "rdf-lens";
import { Bucket, Record } from "../utils";
import { TREE } from "@treecg/types";
import { namedNode } from "../core";
import { PagedBucketizer, SubjectBucketizer } from "./bucketizers";

export const SHAPES_FILE_LOCATION = path.join(__dirname, "shapes.ttl");
export const SHAPES_TEXT = readFileSync(SHAPES_FILE_LOCATION, {
  encoding: "utf8",
});

export type Config = {
  type: Term;
  config: SubjectFragmentation | PageFragmentation | TimebasedFragmentation;
};

export type SubjectFragmentation = {
  path: BasicLensM<Cont, Cont>;
  pathQuads: Cont;
};

export type PageFragmentation = {
  pageSize: number;
};

export type TimebasedFragmentation = {
  path: BasicLensM<Cont, Cont>;
  maxGranularity: "day" | "hour" | "second";
};

export interface Bucketizer {
  bucketize(
    sdsMember: Record,
    getBucket: (key: string, root?: boolean) => Bucket,
  ): Bucket[];

  save(): string;
}

type Save = { [key: string]: { bucketizer?: Bucketizer; save?: string } };

function createBucketizer(config: Config, save?: string): Bucketizer {
  switch (config.type.value) {
    case TREE.custom("SubjectFragmentation"):
      return new SubjectBucketizer(<SubjectFragmentation>config.config, save);
    case TREE.custom("PageFragmentation"):
      return new PagedBucketizer(<PageFragmentation>config.config, save);
    case TREE.custom("TimebasedFragmentation"):
      throw "Not yet implemented";
  }
  throw "Unknown bucketizer " + config.type.value;
}

export class BucketizerOrchestrator {
  private readonly configs: Config[];

  private saves: Save = {};

  constructor(configs: Config[], save?: string) {
    this.configs = configs;

    if (save) {
      this.saves = JSON.parse(save);
    }
  }

  bucketize(record: Record, buckets: { [id: string]: Bucket }): string[] {
    let queue = [""];

    for (let i = 0; i < this.configs.length; i++) {
      const todo = queue.slice();
      queue = [];

      for (let prefix of todo) {
        const bucketizer = this.getBucketizer(i, prefix);

        const foundBucket = bucketizer.bucketize(record, (key, root) => {
          const id = prefix + "/" + key;
          if (!buckets[id]) {
            buckets[id] = new Bucket(namedNode(id), [], root);
          }
          return buckets[id];
        });

        for (let bucket of foundBucket) {
          queue.push(bucket.id.value);
        }
      }
    }

    return queue;
  }

  save(): string {
    for (let key of Object.keys(this.saves)) {
      this.saves[key].save = this.saves[key].bucketizer!.save();
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
