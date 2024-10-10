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
    TimeBucketTreeConfig,
} from "../lib/bucketizers/index";
import { Bucket, Record } from "../lib/";
import { BucketRelation } from "../lib/utils";

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
        const output = <BucketizerConfig>lens.execute({
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
        const output = <BucketizerConfig>lens.execute({
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
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.

<test> <tsp> "2024-01-01T00:00:00Z"^^xsd:dateTime.

<a> a tree:TimebasedFragmentation;
  tree:timestampPath <tsp>;
  tree:maxSize 3;
  tree:k 2;
  tree:minBucketSpan 3600.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#TimebasedFragmentation",
        );
        const config = <TimebasedFragmentation>output.config;
        expect(config.path).toBeDefined();

        expect(config.maxSize).toEqual(3);
        expect(config.k).toEqual(2);
        expect(config.minBucketSpan).toEqual(3600);

        const applied = config.path.execute({ id: namedNode("test"), quads });
        expect(applied.map((x) => x.id.value)).toEqual([
            "2024-01-01T00:00:00Z",
        ]);
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
        const output = <BucketizerConfig>lens.execute({
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
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const recordBuckets: string[] = [];
        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a3"), quads: [] }, stream),
        ]) {
            recordBuckets.push(
                ...orchestrator.bucketize(
                    member,
                    buckets,
                    requestedBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        expect(recordBuckets).toEqual(["", "", "page-1/"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(1);
        expect(buckets["page-1/"].links.length).toBe(0);
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
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const recordBuckets: string[] = [];
        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
        ]) {
            recordBuckets.push(
                ...orchestrator.bucketize(
                    member,
                    buckets,
                    requestedBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        expect(recordBuckets).toEqual(["a1/", "a2/", "a2/"]);
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
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
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
            recordBuckets.push(
                ...orchestrator.bucketize(
                    member,
                    buckets,
                    requestedBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        expect(recordBuckets).toEqual(["test-a1/", "test-a1/", "test-a2/"]);
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
        const config1 = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });
        const config2 = <BucketizerConfig>lens.execute({
            id: namedNode("b"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([config1, config2]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const recordBuckets: string[] = [];

        for (const member of [
            new Record({ id: namedNode("a1"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
            new Record({ id: namedNode("a2"), quads: [] }, stream),
        ]) {
            recordBuckets.push(
                ...orchestrator.bucketize(
                    member,
                    buckets,
                    requestedBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        expect(Object.keys(buckets).length).toBe(4);
        expect(recordBuckets).toEqual(["a1/", "a2/", "a2/", "a2/page-1/"]);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets["a2/"].parent!.id.value).toBe("");
        expect(buckets["a2/page-1/"].parent!.id.value).toBe("a2/");
        expect(buckets["a1/"].parent!.id.value).toBe("");
    });

    describe("timebucket", () => {
        const dayMs = 1000 * 60 * 60 * 24;
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a tree:TimeBucketFragmentation;
  tree:timestampPath <time>;
  tree:level ( 
    [tree:range "year";  tree:maxSize 1]
    [tree:range "month"; tree:maxSize 2]
  );
  tree:buffer ${dayMs}. # one day
`;
        let idCount = 0;
        const stream = namedNode("MyStream");
        const record = (date: Date) => {
            const id = namedNode("a" + idCount);
            idCount += 1;

            return new Record(
                {
                    id,
                    quads: [
                        quad(
                            id,
                            namedNode("time"),
                            literal(date.toISOString()),
                        ),
                    ],
                },
                stream,
            );
        };

        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const config = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });
        const inner = <TimeBucketTreeConfig>config.config;

        test("correct config", () => {
            expect(inner.levels).toEqual([
                { range: "year", amount: 1 },
                {
                    range: "month",
                    amount: 2,
                },
            ]);
            expect(inner.timeBufferMs).toBe(dayMs);
        });

        const orchestrator = new BucketizerOrchestrator([config]);

        const buckets: { [id: string]: Bucket } = {};
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const recordBuckets: string[][] = [];

        const firstBuckets = new Set<string>();
        for (const member of [
            record(new Date(2024, 1, 1)),
            // record(new Date(2024, 1, 2)),
        ]) {
            recordBuckets.push(
                orchestrator.bucketize(
                    member,
                    buckets,
                    firstBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        test("First bucket is the year", () => {
            expect(firstBuckets).toEqual(new Set(["", "2024/"]));
            expect(recordBuckets[0]).toEqual(["2024/"]);
            // expect(recordBuckets[1]).toEqual(["2024/"]);
        });

        const secondBuckets = new Set<string>();
        for (const member of [
            record(new Date(2024, 1, 2)),
            record(new Date(2024, 2, 3)),
        ]) {
            recordBuckets.push(
                orchestrator.bucketize(
                    member,
                    buckets,
                    secondBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        test("Second bucket is the month", () => {
            expect(secondBuckets).toEqual(
                new Set(["", "2024/", "2024/march/", "2024/february/"]),
            );
            expect(recordBuckets[1]).toEqual(["2024/february/"]);
            expect(recordBuckets[2]).toEqual(["2024/march/"]);

            expect(buckets["2024/february/"].immutable).toBeTruthy();
            expect(buckets["2024/march/"].immutable).toBeFalsy();
            expect(buckets["2024/"].immutable).toBeFalsy();
            expect(buckets[""].immutable).toBeFalsy();
        });

        const restBuckets = new Set<string>();
        for (const member of [record(new Date(2024, 3, 2))]) {
            recordBuckets.push(
                orchestrator.bucketize(
                    member,
                    buckets,
                    restBuckets,
                    newMembers,
                    newRelations,
                ),
            );
        }

        test("second bucket is april, yet february is not yet closed", () => {
            expect(recordBuckets[3]).toEqual(["2024/april/"]);
            expect(buckets["2024/march/"].immutable).toBeFalsy();
            expect(buckets["2024/april/"].immutable).toBeFalsy();
        });
    });
});
