import { describe, expect, test } from "vitest";
import { DataFactory, Parser } from "n3";
import { extractShapes } from "rdf-lens";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    PageFragmentation,
    SHAPES_TEXT,
    SubjectFragmentation,
    TimebasedFragmentation,
} from "../src/bucketizers/index";
import { Bucket, Record } from "../src/utils";

const { namedNode, literal, quad } = DataFactory;

describe("Bucketizer configs", () => {
    const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);

    const shapes = extractShapes(quads);
    const lens = shapes.lenses["https://w3id.org/tree#FragmentationStrategy"];

    test("Subject Page", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<test> <b> [
  <c> 42;
].

<a> a tree:SubjectFragmentation;
  tree:fragmentationPath (<b> <c>).
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#SubjectFragmentation",
        );
        const config = <SubjectFragmentation>output.config;
        expect(config.path).toBeDefined();

        const applied = config.path.execute({ id: namedNode("test"), quads });
        expect(applied.map((x) => x.id.value)).toEqual(["42"]);

        expect(config.pathQuads).toBeDefined();
        expect(config.pathQuads.id.termType).toBe("BlankNode");
        expect(config.pathQuads.quads.length).toBe(4);
    });

    test("Subject Page - simple path", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<test> <b> [
  <c> 42;
].

<a> a tree:SubjectFragmentation;
  tree:fragmentationPath <b>.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#SubjectFragmentation",
        );
        const config = <SubjectFragmentation>output.config;
        expect(config.path).toBeDefined();

        expect(config.pathQuads).toBeDefined();
        expect(config.pathQuads.id.termType).toBe("NamedNode");
        expect(config.pathQuads.quads.length).toBe(0);
    });

    test("Timebased", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<test> <b> [
  <c> 42;
].

<a> a tree:TimebasedFragmentation;
  tree:maxGranularity "hour";
  tree:fragmentationPath (<b> <c>).
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#TimebasedFragmentation",
        );
        const config = <TimebasedFragmentation>output.config;
        expect(config.path).toBeDefined();

        expect(config.maxGranularity).toEqual("hour");

        const applied = config.path.execute({ id: namedNode("test"), quads });
        expect(applied.map((x) => x.id.value)).toEqual(["42"]);
    });

    test("Paged", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.


<a> a tree:PageFragmentation;
  tree:pageSize 42.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#PageFragmentation",
        );
        const config = <PageFragmentation>output.config;
        expect(config.pageSize).toBe(42);
    });
});

describe("Bucketizer behavior", () => {
    const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);

    const shapes = extractShapes(quads);
    const lens = shapes.lenses["https://w3id.org/tree#FragmentationStrategy"];

    test("Paged", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.


<a> a tree:PageFragmentation;
  tree:pageSize 2.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const recordBuckets: string[] = [];
        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a3"), quads: [] }, stream),
        ]) {
            recordBuckets.push(...orchestrator.bucketize(member, buckets));
        }

        expect(recordBuckets).toEqual(["", "", "/page-1"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(1);
        expect(buckets["/page-1"].links.length).toBe(0);
    });

    test("Subject", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a tree:SubjectFragmentation;
  tree:fragmentationPath ( ).
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const recordBuckets: string[] = [];
        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
        ]) {
            recordBuckets.push(...orchestrator.bucketize(member, buckets));
        }

        expect(recordBuckets).toEqual(["/a1", "/a2", "/a2"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);
    });

    test("Subject with name", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a tree:SubjectFragmentation;
  tree:fragmentationPath ( );
  tree:fragmentationPathName ex:test.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const recordBuckets: string[] = [];
        const pred = namedNode("http://example.org/test");
        for (const member of [
            new Record(
                {
                    id: namedNode("a1"),
                    quads: [quad(namedNode("a1"), pred, literal("test-a1"))],
                },
                stream,
            ),
            new Record(
                {
                    id: namedNode("a2"),
                    quads: [quad(namedNode("a2"), pred, literal("test-a1"))],
                },
                stream,
            ),
            new Record(
                {
                    id: namedNode("a3"),
                    quads: [quad(namedNode("a3"), pred, literal("test-a2"))],
                },
                stream,
            ),
        ]) {
            recordBuckets.push(...orchestrator.bucketize(member, buckets));
        }

        expect(recordBuckets).toEqual(["/test-a1", "/test-a1", "/test-a2"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);
    });

    test("Combined", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a tree:SubjectFragmentation;
  tree:fragmentationPath ( ).

<b> a tree:PageFragmentation;
  tree:pageSize 2.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const config1: BucketizerConfig = lens.execute({
            id: namedNode("a"),
            quads,
        });
        const config2: BucketizerConfig = lens.execute({
            id: namedNode("b"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([config1, config2]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const recordBuckets: string[] = [];

        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
        ]) {
            recordBuckets.push(...orchestrator.bucketize(member, buckets));
        }

        expect(Object.keys(buckets).length).toBe(4);
        expect(recordBuckets).toEqual(["/a1", "/a2", "/a2", "/a2/page-1"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets["/a2"].parent!.id.value).toBe("");
        expect(buckets["/a2/page-1"].parent!.id.value).toBe("/a2");
        expect(buckets["/a1"].parent!.id.value).toBe("");
    });
});
