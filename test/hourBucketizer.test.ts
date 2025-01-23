import { beforeEach, describe, expect, test } from "vitest";
import { DataFactory, Parser } from "n3";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    SHAPES_TEXT,
} from "../lib/bucketizers/index";
import { extractShapes } from "rdf-lens";
import { Bucket, Record } from "../lib";
import { BucketRelation } from "../src/utils";

const { namedNode } = DataFactory;

type Member = { id: string; timestamp: Date; text: string };

describe("HourBucketizer tests", () => {
    function memberToRecord(member: Member): Record {
        const quadsStr = `
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:${member.id} a ex:Member ;
    ex:timestamp "${member.timestamp.toISOString()}"^^xsd:dateTime ;
    ex:text "${member.text}" .
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        return new Record(
            {
                id: namedNode(`http://example.org/${member.id}`),
                quads,
            },
            namedNode("http://example.org/MyStream"),
        );
    }

    const bucketizerConfigsQuads = new Parser({ baseIRI: "" }).parse(
        SHAPES_TEXT,
    );
    const shapes = extractShapes(bucketizerConfigsQuads);
    const lens = shapes.lenses["https://w3id.org/tree#FragmentationStrategy"];

    function getOrchestrator(save?: string) {
        const quadsStr = `
@prefix ex: <http://example.org/> .
@prefix tree: <https://w3id.org/tree#> .

ex:Fragmentation a tree:HourFragmentation ;
    tree:timestampPath ex:timestamp .
`;

        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = <BucketizerConfig>lens.execute({
            id: namedNode("http://example.org/Fragmentation"),
            quads,
        });

        return new BucketizerOrchestrator([output], save);
    }

    let buckets: { [id: string]: Bucket } = {};

    beforeEach(() => {
        buckets = {};
    });

    test("member in next hour should create new bucket", () => {
        const orchestrator = getOrchestrator();

        const record1 = memberToRecord({
            id: "m1",
            timestamp: new Date("2024-04-01T09:05:00Z"),
            text: "First member",
        });
        const member2 = {
            id: "m2",
            timestamp: new Date("2024-04-01T10:10:00Z"),
            text: "Second member",
        };
        const record2 = memberToRecord(member2);

        const firstBucketExpected = "";
        const secondBucketExpected = `${encodeURIComponent(
            new Date(
                member2.timestamp.getFullYear(),
                member2.timestamp.getMonth(),
                member2.timestamp.getDate(),
                member2.timestamp.getHours(),
            ).toISOString(),
        )}/`;

        // Insert first record
        const record1Buckets = orchestrator.bucketize(
            record1,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            "",
        );

        expect(record1Buckets.length).toBe(1);
        expect(record1Buckets[0]).toBe(firstBucketExpected);

        // Insert second record
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const record2Buckets = orchestrator.bucketize(
            record2,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            newRelations,
            "",
        );

        expect(record2Buckets.length).toBe(1);
        expect(record2Buckets[0]).toBe(secondBucketExpected);

        expect(newRelations.length).toBe(2);
        expect(newRelations[0].origin.id.value).toBe(firstBucketExpected);
        expect(newRelations[0].relation.target.value).toBe(
            secondBucketExpected,
        );

        expect(newRelations[1].origin.id.value).toBe(secondBucketExpected);
        expect(newRelations[1].relation.target.value).toBe(firstBucketExpected);
    });

    test("member in same hour should not create new bucket", () => {
        const orchestrator = getOrchestrator();

        const record1 = memberToRecord({
            id: "m1",
            timestamp: new Date("2024-04-01T09:05:00Z"),
            text: "First member",
        });
        const member2 = {
            id: "m2",
            timestamp: new Date("2024-04-01T09:10:00Z"),
            text: "Second member",
        };
        const record2 = memberToRecord(member2);

        const firstBucketExpected = "";
        const secondBucketExpected = "";

        // Insert first record
        const record1Buckets = orchestrator.bucketize(
            record1,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            "",
        );

        expect(record1Buckets.length).toBe(1);
        expect(record1Buckets[0]).toBe(firstBucketExpected);

        // Insert second record
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const record2Buckets = orchestrator.bucketize(
            record2,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            newRelations,
            "",
        );

        expect(record2Buckets.length).toBe(1);
        expect(record2Buckets[0]).toBe(secondBucketExpected);

        expect(newRelations.length).toBe(0);
    });

    test("member in future hour (with a gap) should create new bucket for that future hour", () => {
        const orchestrator = getOrchestrator();

        const record1 = memberToRecord({
            id: "m1",
            timestamp: new Date("2024-04-01T09:05:00Z"),
            text: "First member",
        });
        const member2 = {
            id: "m2",
            timestamp: new Date("2024-04-01T14:10:00Z"),
            text: "Second member",
        };
        const record2 = memberToRecord(member2);

        const firstBucketExpected = "";
        const secondBucketExpected = `${encodeURIComponent(
            new Date(
                member2.timestamp.getFullYear(),
                member2.timestamp.getMonth(),
                member2.timestamp.getDate(),
                member2.timestamp.getHours(),
            ).toISOString(),
        )}/`;

        // Insert first record
        const record1Buckets = orchestrator.bucketize(
            record1,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            "",
        );

        expect(record1Buckets.length).toBe(1);
        expect(record1Buckets[0]).toBe(firstBucketExpected);

        // Insert second record
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const record2Buckets = orchestrator.bucketize(
            record2,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            newRelations,
            "",
        );

        expect(record2Buckets.length).toBe(1);
        expect(record2Buckets[0]).toBe(secondBucketExpected);

        expect(newRelations.length).toBe(2);
        expect(newRelations[0].origin.id.value).toBe(firstBucketExpected);
        expect(newRelations[0].relation.target.value).toBe(
            secondBucketExpected,
        );

        expect(newRelations[1].origin.id.value).toBe(secondBucketExpected);
        expect(newRelations[1].relation.target.value).toBe(firstBucketExpected);
    });
});
