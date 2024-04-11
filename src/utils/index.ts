import { Quad, Term } from "@rdfjs/types";
import { blankNode, literal, quad } from "../core";
import { Parser, Quad_Object, Quad_Subject, Writer } from "n3";
import { SDS, XSD } from "@treecg/types";
import { BasicLensM, extractShapes, match, Shapes, subject } from "rdf-lens";
import { readFileSync } from "fs";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";
import * as path from "path";

export const SHAPES_FILE_LOCATION = path.join(__dirname, "../../configs/sds_shapes.ttl");
export const SHAPES_TEXT = readFileSync(SHAPES_FILE_LOCATION, {
  encoding: "utf8",
});

export type RdfThing = {
  id: Term;
  quads: Quad[];
};

export type RelationDTO = {
  type: Term;
  target: BucketDTO;
  value: any;
  path: Term;
};

export type BucketDTO = {
  links: RelationDTO[];
  id: Term;
  root: boolean;
};

export type BucketRelation = {
  type: Term;
  target: Bucket;
  value?: Term;
  path?: RdfThing;
};

function writeRelation(rel: BucketRelation, writer: Writer): Term {
  const id = blankNode();
  writer.addQuads([
    quad(id, SDS.terms.relationType, <Quad_Object>rel.type),
    quad(id, SDS.terms.relationBucket, <Quad_Object>rel.target.id),
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

  constructor(id: Term, links: BucketRelation[], root?: boolean) {
    this.id = id;
    this.root = root;
    this.links = links;
  }

  static parse(bucket: BucketDTO): Bucket {
    const links = bucket.links.map(({ target, path, value, type }) => ({
      path: { id: path, quads: [] },
      value,
      type,
      target: Bucket.parse(target),
    }));
    return new Bucket(bucket.id, links, bucket.root);
  }

  addRelation(target: Bucket, type: Term, value?: Term, path?: RdfThing) {
    this.links.push({ type, value, path, target });
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
          SDS.terms.custom("root"),
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
  ): Promise<Record> {
    const thingQuads = await extractor.extract(store, data);

    if (bucket) {
      bucket = Bucket.parse(bucket);
    }

    return new Record({ id: data, quads: thingQuads }, stream, bucket);
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

  constructor(extractor: CBDShapeExtractor, stream?: Term) {
    this.extractor = extractor;

    const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);
    this.shapes = extractShapes(quads, {
      "#Bucket": (item) => {
        return Bucket.parse(item);
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
        return Record.parse(dto, store, this.extractor);
      }),
    );
  }
}
