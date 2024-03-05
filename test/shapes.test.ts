import { describe, expect, test } from "@jest/globals";
import { shacl } from "rdf-lens";
import { readFileSync } from "fs";
import { DataFactory, Parser } from "n3";
import { CBDShapeExtractor } from "extract-cbd-shape";

import { Bucket, Extractor } from "../src/bucketizers/index";

export const { namedNode, blankNode, literal, quad } = DataFactory;

describe("Extracting defined shapes", async () => {
  const pipeline = readFileSync("./shapes.ttl", { encoding: "utf8" });
  const quads = new Parser({ baseIRI: "" }).parse(pipeline);

  const shapes = shacl.extractShapes(quads);

  test("extracts relation", () => {
    const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a <#Relation>;
  sds:relationType tree:GreaterThanRelation ;
  sds:relationBucket <bucket2> ;
  sds:relationValue 1;
  sds:relationPath ex:x.
`;
    const quads = new Parser({ baseIRI: "" }).parse(quadsStr);

    const relation = shapes.lenses["#Relation"].execute({
      id: namedNode("a"),
      quads,
    });

    const keys = Object.keys(relation);
    expect(keys).toContain("type");
    expect(keys).toContain("target");
    expect(keys).toContain("value");
    expect(keys).toContain("path");
  });

  test("extracts bucket", () => {
    const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<bucket> sds:relation <a>;
  sds:root true.
<a> a <#Relation>;
  sds:relationType tree:GreaterThanRelation ;
  sds:relationBucket <bucket2> ;
  sds:relationValue 1;
  sds:relationPath ex:x.
`;
    const quads = new Parser({ baseIRI: "" }).parse(quadsStr);

    const bucket = shapes.lenses["#Bucket"].execute({
      id: namedNode("bucket"),
      quads,
    });

    const keys = Object.keys(bucket);
    expect(keys).toContain("id");
    expect(keys).toContain("links");
    expect(keys).toContain("root");
    expect(bucket.root).toBe(true);
  });

  test("extracts record", () => {
    const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<record> a sds:Record;
  sds:stream <#stream>;
  sds:bucket <bucket>;
  sds:payload [ ].

<bucket> sds:relation <a>.
<a> a <#Relation>;
  sds:relationType tree:GreaterThanRelation ;
  sds:relationBucket <bucket2> ;
  sds:relationValue 1;
  sds:relationPath ex:x.
`;
    const quads = new Parser({ baseIRI: "" }).parse(quadsStr);

    const record = shapes.lenses["#Record"].execute({
      id: namedNode("record"),
      quads,
    });

    const keys = Object.keys(record);
    expect(keys).toContain("bucket");
    expect(keys).toContain("stream");
    expect(keys).toContain("thing");
  });

  test("defined shapes", async () => {
    expect([...Object.keys(shapes.lenses)]).toContain("#Relation");
    expect([...Object.keys(shapes.lenses)]).toContain("#Bucket");
    expect([...Object.keys(shapes.lenses)]).toContain("#Record");
  });

  test("extract sds records", async () => {
    const extractor = new Extractor(new CBDShapeExtractor());
    await extractor.init();

    const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<record> a sds:Record;
  sds:stream <#stream>;
  sds:bucket <bucket>;
  sds:payload [
    <x> 42;
    <y> 45;
  ].

<bucket> sds:relation <a>; a <#Bucket>.
<a> a <#Relation>;
  sds:relationType tree:GreaterThanRelation ;
  sds:relationBucket <bucket2> ;
  sds:relationValue 1;
  sds:relationPath ex:x.
`;
    const quads = new Parser({ baseIRI: "" }).parse(quadsStr);

    const out = await extractor.parse_records(quads);
    expect(out.length).toBe(1);
    expect(out[0].thing.quads.length).toBe(2);

    expect(out[0].bucket).toBeInstanceOf(Bucket);
  })
});

