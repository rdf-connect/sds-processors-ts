import { BasicLensM, Cont } from "rdf-lens";
import { Bucket, RdfThing, Record } from "../utils";
import { TREE } from "@treecg/types";
import { Bucketizer, PageFragmentation, SubjectFragmentation } from ".";

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
      oldBucket.addRelation(currentbucket, TREE.terms.Relation);
    }

    return [currentbucket];
  }

  save() {
    return JSON.stringify(this.count);
  }
}

export class SubjectBucketizer implements Bucketizer {
  private readonly path: BasicLensM<Cont, Cont>;
  private readonly pathQuads: RdfThing;

  private seen: Set<string> = new Set();

  constructor(config: SubjectFragmentation, save?: string) {
    this.path = config.path;
    this.pathQuads = config.pathQuads;
    if (save) {
      this.seen = new Set(JSON.parse(save));
    }
  }

  bucketize(
    record: Record,
    getBucket: (key: string, root?: boolean) => Bucket,
  ): Bucket[] {
    const values = this.path.execute(record.data);
    const out: Bucket[] = [];

    const root = getBucket("root", true);

    for (let value of values) {
      const bucket = getBucket("bucket-" + value.id.value);

      if (!this.seen.has(value.id.value)) {
        this.seen.add(value.id.value);

        root.addRelation(
          bucket,
          TREE.terms.EqualToRelation,
          value.id,
          this.pathQuads,
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
