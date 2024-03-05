import { Quad, Term } from "@rdfjs/types";
import { blankNode, literal, namedNode, quad } from "../core";
import { Parser, Quad_Object, Store, Writer } from "n3";
import { SDS, XSD } from "@treecg/types";
import { BasicLensM, match, shacl } from "rdf-lens";
import { readFileSync } from "fs";
import { CBDShapeExtractor } from "extract-cbd-shape";

export type RdfThing = {
  id: Term;
  quads: Quad[];
};

export type BucketRelation = {
  type: Term;
  target: string;
  value: Term;
  path: RdfThing;
};

function writeRelation(rel: BucketRelation, writer: Writer): Term {
  const id = blankNode();
  writer.addQuads([
    quad(id, SDS.terms.relationType, <Quad_Object>rel.type),
    quad(id, SDS.terms.relationBucket, namedNode(rel.target)),
    quad(id, SDS.terms.relationValue, <Quad_Object>rel.value),
    quad(id, SDS.terms.relationPath, <Quad_Object>rel.path.id),
    ...rel.path.quads,
  ]);
  return id;
}

export class Bucket {
  id: string;
  root?: boolean;
  links: BucketRelation[];

  constructor(id: string, links: BucketRelation[], root?: boolean) {
    this.id = id;
    this.root = root;
    this.links = links;
  }

  static parse(bucket: { [field: string]: any }): Bucket {
    return new Bucket(bucket.id, bucket.links, bucket.root);
  }

  addRelation(target: Bucket, type: Term, value: Term, path: RdfThing) {
    this.links.push({ type, value, path, target: target.id });
  }

  write(writer: Writer) {
    const id = namedNode(this.id);
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
  thing: Term;
  bucket: Bucket;
}

export class Record {
  stream: Term;
  thing: RdfThing;
  bucket?: Bucket;

  constructor(thing: RdfThing, stream: Term, bucket?: Bucket) {
    this.stream = stream;
    this.thing = thing;
    this.bucket = bucket;
  }

  static async parse(
    { stream, thing, bucket }: RecordDTO,
    store: Store,
    extractor: CBDShapeExtractor,
  ): Promise<Record> {
    const thingQuads = await extractor.extract(store, thing);
    if (bucket) {
      bucket = Bucket.parse(bucket);
    }
    return new Record({ id: thing, quads: thingQuads }, stream, bucket);
  }

  write(writer: Writer) {
    const id = blankNode();

    const quads = [
      quad(id, SDS.terms.payload, <Quad_Object>this.thing.id),
      quad(id, SDS.terms.stream, <Quad_Object>this.stream),
    ];
    if (this.bucket) {
      quads.push(quad(id, SDS.terms.bucket, namedNode(this.bucket.id)));
      this.bucket.write(writer);
    }
    writer.addQuads(quads);
  }
}

export class Extractor {
  shapes!: shacl.Shapes;
  lens!: BasicLensM<Quad[], RecordDTO>;
  extractor: CBDShapeExtractor;

  constructor(extractor: CBDShapeExtractor) {
    this.extractor = extractor;
  }

  async init() {
    const pipeline = readFileSync("./shapes.ttl", { encoding: "utf8" });
    const quads = new Parser({ baseIRI: "" }).parse(pipeline);

    this.shapes = shacl.extractShapes(quads);

    this.lens = <BasicLensM<Quad[], RecordDTO>>match(
      undefined,
      SDS.terms.stream,
      undefined,
    )
      .mapAll(({ id, quads }) => ({ id: id.subject, quads }))
      .thenSome(this.shapes.lenses["#Record"])
      .mapAll((x) => <RecordDTO>x);
  }

  async parse_records(quads: Quad[]): Promise<Record[]> {
    const dtos = this.lens.execute(quads);
    const store = new Store(quads);
    return await Promise.all(
      dtos.map((dto) => Record.parse(dto, store, this.extractor)),
    );
  }
}
