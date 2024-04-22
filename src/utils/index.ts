import { Quad, Term } from "@rdfjs/types";
import { blankNode, literal, namedNode, NBNode, quad } from "../core";
import { Parser, Quad_Object, Quad_Subject, Writer } from "n3";
import { SDS, XSD } from "@treecg/types";
import { BasicLensM, extractShapes, match, Shapes, subject } from "rdf-lens";
import { readFileSync } from "fs";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";
import * as path from "path";

export const SDS_GRAPH = SDS.terms.custom("DataDescription");
export const SHAPES_FILE_LOCATION = path.join(
  __dirname,
  "../../configs/sds_shapes.ttl",
);
export const SHAPES_TEXT = readFileSync(SHAPES_FILE_LOCATION, {
  encoding: "utf8",
});

export type RdfThing = {
  id: Term;
  quads: Quad[];
};

export type RelationDTO = {
  type: Term;
  target: Term;
  value?: any;
  path?: RdfThing;
};

export type BucketDTO = {
  links: RelationDTO[];
  id: Term;
  root?: boolean;
  immutable?: boolean;
  parent?: BucketDTO;
};

export type BucketRelation = {
  type: Term;
  target: Term;
  value?: Term;
  path?: RdfThing;
};

function writeRelation(rel: BucketRelation, writer: Writer): Term {
  const id = blankNode();
  writer.addQuads([
    quad(id, SDS.terms.relationType, <Quad_Object>rel.type),
    quad(id, SDS.terms.relationBucket, <Quad_Object>rel.target),
  ]);

  if (rel.value) {
    writer.addQuad(quad(id, SDS.terms.relationValue, <Quad_Object>rel.value));
  }
  if (rel.path) {
    writer.addQuads([
      quad(id, SDS.terms.relationPath, <Quad_Object>rel.path.id),
      ...rel.path.quads,
    ]);
  }
  return id;
}

export class Bucket {
  id: Term;
  parent?: Bucket;
  root?: boolean;
  immutable?: boolean;
  links: BucketRelation[];

  constructor(
    id: Term,
    links: BucketRelation[],
    root?: boolean,
    immutable?: boolean,
    parent?: Bucket,
  ) {
    this.id = id;
    this.root = root;
    this.immutable = immutable;
    this.links = links;
    this.parent = parent;
  }

  static parse(
    bucket: BucketDTO,
    bucket_cache: { [id: string]: Bucket },
  ): Bucket {
    const parent = bucket.parent
      ? Bucket.parse(bucket.parent, bucket_cache)
      : undefined;
    const links = bucket.links.map(({ target, path, value, type }) => ({
      path,
      value,
      type,
      target: target,
    }));
    const out = new Bucket(
      bucket.id,
      links,
      bucket.root,
      bucket.immutable,
      parent,
    );
    bucket_cache[bucket.id.value] = out;
    return out;
  }

  addRelation(target: Bucket, type: Term, value?: Term, path?: RdfThing) {
    this.links.push({ type, value, path, target: target.id });
    target.parent = this;
  }

  write(writer: Writer) {
    const id = <Quad_Subject>this.id;
    const relations = this.links
      .map((rel) => writeRelation(rel, writer))
      .map((rel) => quad(id, SDS.terms.relation, <Quad_Object>rel));

    if (this.root) {
      relations.push(
        quad(
          id,
          SDS.terms.custom("isRoot"),
          literal("true", XSD.terms.custom("boolean")),
        ),
      );
    }

    writer.addQuads(relations);
  }
}

interface RecordDTO {
  stream: Term;
  data: Term;
  bucket: any;
}

export class Record {
  stream: Term;
  data: RdfThing;
  bucket?: Bucket;

  constructor(data: RdfThing, stream: Term, bucket?: Bucket) {
    this.stream = stream;
    this.data = data;
    this.bucket = bucket;
  }

  static async parse(
    { stream, data, bucket }: RecordDTO,
    store: RdfStore,
    extractor: CBDShapeExtractor,
    bucket_cache: { [id: string]: Bucket },
    shape?: NBNode,
  ): Promise<Record> {
    const thingQuads = await extractor.extract(store, data, shape);

    let actual_bucket: Bucket | undefined;
    if (bucket) {
      actual_bucket = Bucket.parse(bucket, bucket_cache);
    }

    return new Record({ id: data, quads: thingQuads }, stream, actual_bucket);
  }

  write(writer: Writer) {
    const id = blankNode();

    const quads = [
      quad(id, SDS.terms.payload, <Quad_Object>this.data.id),
      quad(id, SDS.terms.stream, <Quad_Object>this.stream),
    ];
    if (this.bucket) {
      quads.push(quad(id, SDS.terms.bucket, <Quad_Object>this.bucket.id));
      this.bucket.write(writer);
    }
    writer.addQuads(quads);
  }
}

export class Extractor {
  extractor: CBDShapeExtractor;
  shapes: Shapes;
  lens: BasicLensM<Quad[], RecordDTO>;
  shape?: NBNode;

  bucket_cache: { [id: string]: Bucket } = {};

  constructor(extractor: CBDShapeExtractor, stream?: Term) {
    this.extractor = extractor;

    const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);
    this.shapes = extractShapes(quads, {
      "#Bucket": (item) => {
        return Bucket.parse(item, this.bucket_cache);
      },
    });

    this.lens = <BasicLensM<Quad[], RecordDTO>>match(
      undefined,
      SDS.terms.stream,
      stream,
    )
      .thenAll(subject)
      .thenSome(this.shapes.lenses["#Record"])
      .mapAll((x) => <RecordDTO>x);
  }

  async parse_records(quads: Quad[]): Promise<Record[]> {
    const store = RdfStore.createDefault();
    quads.forEach((quad) => store.addQuad(quad));

    const dtos = this.lens.execute(quads);

    return await Promise.all(
      dtos.map((dto) => {
        return Record.parse(
          dto,
          store,
          this.extractor,
          this.bucket_cache,
          this.shape,
        );
      }),
    );
  }
}
