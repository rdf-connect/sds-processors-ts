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

describe("reversedPagedBucketizer tests", () => {
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

    function getOrchestrator(pageSize: number, save?: string) {
        const quadsStr = `
@prefix ex: <http://example.org/> .
@prefix tree: <https://w3id.org/tree#> .

ex:Fragmentation a tree:ReversedPageFragmentation ;
    tree:pageSize ${pageSize} ;
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

    test("bucketizing 1 member with pageSize=2 creates 1 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(2);

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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        expect.assertions(4);
    });

    test("bucketizing 2 members with pageSize=2 creates 1 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(2);

        for (let i = 0; i < 2; i++) {
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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        expect.assertions(8);
    });

    test("bucketizing 4 members with pageSize=2 creates 2 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(2);

        for (let i = 0; i < 2; i++) {
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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        for (let i = 2; i < 4; i++) {
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
            expect(recordBuckets[0]).toBe("page-1/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-1/");
        }

        expect.assertions(16);
    });

    test("bucketizing 6 members with pageSize=2 creates 3 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(2);

        for (let i = 0; i < 2; i++) {
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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        for (let i = 2; i < 4; i++) {
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
            expect(recordBuckets[0]).toBe("page-1/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-1/");
        }

        for (let i = 4; i < 6; i++) {
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
            expect(recordBuckets[0]).toBe("page-2/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-2/");
        }

        expect.assertions(24);
    });

    test("bucketizing 6 members with pageSize=3 creates 2 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(3);

        for (let i = 0; i < 3; i++) {
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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        for (let i = 3; i < 6; i++) {
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
            expect(recordBuckets[0]).toBe("page-1/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-1/");
        }

        expect.assertions(24);
    });

    test("bucketizing 6 members with pageSize=4 creates 2 page bucket and navigator bucket", () => {
        const orchestrator = getOrchestrator(4);

        for (let i = 0; i < 4; i++) {
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
            expect(recordBuckets[0]).toBe("page-0/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-0/");
        }

        for (let i = 4; i < 6; i++) {
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
            expect(recordBuckets[0]).toBe("page-1/");

            expect(buckets[""].links.length).toBe(1);
            expect(buckets[""].links[0].target.value).toBe("page-1/");
        }

        expect.assertions(24);
    });
});
