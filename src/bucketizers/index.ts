import { readFileSync } from "fs";
import * as path from "path";
import { Term } from "rdf-js";
import { BasicLensM, Cont } from "rdf-lens";
import { Bucket, Record } from "../utils";
import { TREE } from "@treecg/types";
import { namedNode } from "../core";
import { PagedBucketizer, SubjectBucketizer } from "./bucketizers";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
export const SHAPES_FILE_LOCATION = path.join(
  __dirname,
  "../../configs/bucketizer_configs.ttl",
);
export const SHAPES_TEXT = readFileSync(SHAPES_FILE_LOCATION, {
  encoding: "utf8",
});

export type BucketizerConfig = {
  type: Term;
  config: SubjectFragmentation | PageFragmentation | TimebasedFragmentation;
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

function createBucketizer(config: BucketizerConfig, save?: string): Bucketizer {
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
    prefix = "",
  ): string[] {
    let queue = [prefix];

    for (let i = 0; i < this.configs.length; i++) {
      const todo = queue.slice();
      queue = [];

      for (let prefix of todo) {
        const bucketizer = this.getBucketizer(i, prefix);

        const getBucket = (value: string, root?: boolean) => {
          const terms = value.split("/");
          const key = terms[terms.length - 1]
            .replaceAll("#", "-")
            .replaceAll(" ", "-");
          // If the requested bucket is the root, it actually is the previous bucket
          const id = root ? prefix : prefix + "/" + key;
          if (!buckets[id]) {
            buckets[id] = new Bucket(namedNode(id), [], false);
          }
          return buckets[id];
        };

        const foundBucket = bucketizer.bucketize(record, getBucket);

        for (let bucket of foundBucket) {
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
    for (let key of Object.keys(this.saves)) {
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
