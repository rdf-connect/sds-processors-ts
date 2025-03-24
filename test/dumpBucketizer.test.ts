import { beforeEach, describe, expect, test } from "vitest";
import { Bucket, Record } from "../lib";
import { DataFactory, Parser } from "n3";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    SHAPES_TEXT,
} from "../lib/bucketizers/index";
import { extractShapes } from "rdf-lens";

const { namedNode } = DataFactory;

type Member = { id: string; timestamp: Date; text: string };

describe("dumpBucketizer tests", () => {
    const members: { id: string; timestamp: Date; text: string }[] = [];

    // Create 10 members with timestamps 5 minutes apart
    for (let i = 0; i < 10; i++) {
        const timestamp = new Date("2025-04-01T09:00:00Z");
        timestamp.setMinutes(timestamp.getMinutes() + i * 5);
        members.push({
            id: `m${i}`,
            timestamp: timestamp,
            text: `Member ${i}`,
        });
    }

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

    function getOrchestrator(includeTimestampPath: boolean, save?: string) {
        let quadsStr = `
@prefix ex: <http://example.org/> .
@prefix tree: <https://w3id.org/tree#> .

ex:Fragmentation a tree:DumpFragmentation .
`;
        if (includeTimestampPath) {
            quadsStr += `
ex:Fragmentation tree:timestampPath ex:timestamp .
`;
        }

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

    test("bucketizing 1 member places it in the root", () => {
        const orchestrator = getOrchestrator(true);

        for (let i = 0; i < 1; i++) {
            const record = memberToRecord(members[i]);
            const recordBuckets = orchestrator.bucketize(
                record,
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
            );

            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe("");

            expect(buckets[""].links.length).toBe(0);
            expect(buckets[""].root).toBe(true);
        }

        expect.assertions(4);
    });

    test("bucketizing 10 members places all of them in the root", () => {
        const orchestrator = getOrchestrator(true);

        for (let i = 0; i < 10; i++) {
            const record = memberToRecord(members[i]);
            const recordBuckets = orchestrator.bucketize(
                record,
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
            );

            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe("");

            expect(buckets[""].links.length).toBe(0);
            expect(buckets[""].root).toBe(true);
        }

        expect.assertions(40);
    });

    test("bucketizing 2 members in the wrong order with a timestamp path returns null", () => {
        const orchestrator = getOrchestrator(true);

        const record1 = memberToRecord(members[1]);
        const record2 = memberToRecord(members[0]);

        const recordBuckets1 = orchestrator.bucketize(
            record1,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            [],
            "",
        );

        expect(recordBuckets1.length).toBe(1);
        expect(recordBuckets1[0]).toBe("");

        const recordBuckets2 = orchestrator.bucketize(
            record2,
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            [],
            "",
        );

        expect(buckets[""].links.length).toBe(0);
        expect(recordBuckets2).toStrictEqual([]);
    });

    test("bucketizing 2 members in the wrong order without a timestamp path places them in the root", () => {
        const orchestrator = getOrchestrator(false);

        for (let i = 1; i >= 0; i--) {
            const record = memberToRecord(members[i]);
            const recordBuckets = orchestrator.bucketize(
                record,
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
            );

            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe("");

            expect(buckets[""].links.length).toBe(0);
            expect(buckets[""].root).toBe(true);
        }

        expect.assertions(8);
    });
});
