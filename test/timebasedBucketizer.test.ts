import { beforeEach, describe, expect, test } from "vitest";
import { DataFactory, Parser } from "n3";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    SHAPES_TEXT,
} from "../src/bucketizers/index";
import { Bucket, Record } from "../src/";
import { extractShapes } from "rdf-lens";
import { Term } from "@rdfjs/types";
import namedNode = DataFactory.namedNode;

describe("TimebasedBucketizer tests", () => {
    const members = [];
    const startDate = new Date("2023-01-01T00:00:00Z");
    const endDate = new Date("2023-12-31T23:59:59Z");

    for (
        let month = startDate.getUTCMonth();
        month <= endDate.getUTCMonth();
        month++
    ) {
        const firstOfMonth = new Date(startDate);
        firstOfMonth.setUTCMonth(month);
        const seventeenthOfMonth = new Date(firstOfMonth);
        seventeenthOfMonth.setUTCDate(17);

        const firstMemberId = `abcd-beginning-of-${firstOfMonth.toISOString("default", { month: "long" })}-${firstOfMonth.getFullYear()}-efgh`;
        const secondMemberId = `ijkl-middle-of-${seventeenthOfMonth.toLocaleString("default", { month: "long" })}-${seventeenthOfMonth.getFullYear()}-mnop`;

        members.push({
            id: firstMemberId,
            timestamp: firstOfMonth,
            text: `This is a member that was added at the beginning of ${firstOfMonth.toLocaleString("default", { month: "long" })} ${firstOfMonth.getFullYear()}`,
        });

        members.push({
            id: secondMemberId,
            timestamp: seventeenthOfMonth,
            text: `This is a member that was added in the middle of ${seventeenthOfMonth.toLocaleString("default", { month: "long" })} ${seventeenthOfMonth.getFullYear()}`,
        });
    }

    function memberToRecord(member: { id; timestamp }): Record {
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

    function getOrchestrator(k: number, m: number, s: number, save?: string) {
        const quadsStr = `
@prefix ex: <http://example.org/> .
@prefix tree: <https://w3id.org/tree#> .

ex:Fragmentation a tree:TimebasedFragmentation ;
    tree:k ${k} ;
    tree:maxSize ${m} ;
    tree:minBucketSpan ${s} ;
    tree:timestampPath ex:timestamp .
`;

        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output: BucketizerConfig = lens.execute({
            id: namedNode("http://example.org/Fragmentation"),
            quads,
        });

        return new BucketizerOrchestrator([output], save);
    }

    let buckets: { [id: string]: Bucket } = {};

    beforeEach(() => {
        buckets = {};
    });

    test("bucketize with (k = 4, m = 10, s = 3600) should split the bucket", () => {
        const orchestrator = getOrchestrator(4, 10, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");

        const secondSplitBucketExpected =
            "/" + encodeURIComponent("2023-04-02T06:00:00.000Z_7884000000_0");
        const thirdSplitBucketExpected =
            "/" + encodeURIComponent("2023-07-02T12:00:00.000Z_7884000000_0");
        const fourthSplitBucketExpected =
            "/" + encodeURIComponent("2023-10-01T18:00:00.000Z_7884000000_0");

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // The next 10 member should be added to a new split bucket.
        for (let i = 10; i < 20; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            if (members[i].timestamp < new Date("2023-07-01T12:00:00.000Z")) {
                expect(recordBuckets[0]).toBe(secondSplitBucketExpected);
            } else if (
                members[i].timestamp < new Date("2023-10-01T18:00:00.000Z")
            ) {
                expect(recordBuckets[0]).toBe(thirdSplitBucketExpected);
            } else {
                expect(recordBuckets[0]).toBe(fourthSplitBucketExpected);
            }
            expect(buckets[firstBucketExpected].root).toBeFalsy();
            expect(buckets[firstBucketExpected].links.length).toBe(2 * 4);
        }
    });

    test("bucketize with (k = 4, m = 100, s = 3600) should all add to the first bucket", () => {
        const orchestrator = getOrchestrator(4, 100, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");

        // All the members should fit in the first bucket.
        for (let i = 0; i < 24; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }
    });

    test("bucketize with (k = 4, m = 10, s = 30000000000) should make new pages", () => {
        const orchestrator = getOrchestrator(4, 10, 30000000000);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");
        const secondBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_1");
        const thirdBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_2");

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // The next 10 member should be added to a new page bucket, as we cannot split due to timespan constraints.
        for (let i = 10; i < 20; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(secondBucketExpected);
            expect(buckets[firstBucketExpected].root).toBeFalsy();
            expect(buckets[firstBucketExpected].links.length).toBe(1);
        }

        // The next 4 member should be added to a new page bucket, as we cannot split due to timespan constraints.
        for (let i = 20; i < 24; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(thirdBucketExpected);
            expect(buckets[secondBucketExpected].root).toBeFalsy();
            expect(buckets[secondBucketExpected].links.length).toBe(1);
        }
    });

    test("bucketize with (k = 4, m = 10, s = 3600) should split the bucket after being loaded with saved state", () => {
        const orchestrator = getOrchestrator(4, 10, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");

        const secondSplitBucketExpected =
            "/" + encodeURIComponent("2023-04-02T06:00:00.000Z_7884000000_0");
        const thirdSplitBucketExpected =
            "/" + encodeURIComponent("2023-07-02T12:00:00.000Z_7884000000_0");
        const fourthSplitBucketExpected =
            "/" + encodeURIComponent("2023-10-01T18:00:00.000Z_7884000000_0");

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // Save the state of the buckets
        const save = orchestrator.save();

        buckets = {};

        // Load the state of the buckets
        const orchestrator2 = getOrchestrator(4, 10, 3600, save);

        // The next 10 member should be added to a new split bucket.
        for (let i = 10; i < 20; i++) {
            const recordBuckets = orchestrator2.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            if (members[i].timestamp < new Date("2023-07-01T12:00:00.000Z")) {
                expect(recordBuckets[0]).toBe(secondSplitBucketExpected);
            } else if (
                members[i].timestamp < new Date("2023-10-01T18:00:00.000Z")
            ) {
                expect(recordBuckets[0]).toBe(thirdSplitBucketExpected);
            } else {
                expect(recordBuckets[0]).toBe(fourthSplitBucketExpected);
            }
            expect(buckets[firstBucketExpected].root).toBeFalsy();
            expect(buckets[firstBucketExpected].links.length).toBe(2 * 4);
        }
    });

    test("bucketize with (k = 4, m = 10, s = 3600) should not split the bucket when not correctly being loaded with saved state", () => {
        const orchestrator = getOrchestrator(4, 10, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // Save the state of the buckets
        const save = undefined;

        buckets = {};

        // Load the state of the buckets
        const orchestrator2 = getOrchestrator(4, 10, 3600, save);

        // The next 10 member should be added to a new split bucket.
        for (let i = 10; i < 20; i++) {
            const recordBuckets = orchestrator2.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }
    });

    test("bucketize with (k = 2, m = 3, s = 3600) should recursively split the buckets", () => {
        const orchestrator = getOrchestrator(2, 3, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");
        const secondBucketExpected =
            "/" + encodeURIComponent("2023-02-15T15:00:00.000Z_3942000000_0");

        // First 3 members should fit in the first bucket.
        for (let i = 0; i < 3; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // Fourth member should be added to a new bucket after recursively splitting.
        const recordBuckets = orchestrator.bucketize(
            memberToRecord(members[3]),
            buckets,
            new Map<string, Set<Term>>(),
            new Map<string, Set<string>>(),
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(secondBucketExpected);
    });

    test("bucketize with (k = 2, m = 3, s = 3600) should recursively split the buckets a lot until it needs to make a new page", () => {
        const orchestrator = getOrchestrator(2, 3, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0");
        const secondBucketExpected =
            "/" + encodeURIComponent("2023-01-01T00:00:00.000Z_3849609_1");

        // First 3 members should fit in the first bucket.
        for (let i = 0; i < 3; i++) {
            const member = members[0];
            member.timestamp = new Date(member.timestamp);
            member.timestamp.setUTCMinutes(i);
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(member),
                buckets,
                new Map<string, Set<Term>>(),
                new Map<string, Set<string>>(),
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }

        // Fourth member should be added to a new bucket after recursively splitting and making a new page.
        const fourthMember = members[0];
        fourthMember.timestamp = new Date(fourthMember.timestamp);
        fourthMember.timestamp.setUTCMinutes(3);
        const recordBuckets = orchestrator.bucketize(
            memberToRecord(fourthMember),
            buckets,
            new Map<string, Set<Term>>(),
            new Map<string, Set<string>>(),
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(secondBucketExpected);
        expect(buckets[secondBucketExpected].parent.links.length).toBe(1);
    });

    test("bucketize with (k = 4, m = 100, s = 3600) should add to a first bucket with adjusted timespan for leap year", () => {
        const orchestrator = getOrchestrator(4, 100, 3600);

        const firstBucketExpected =
            "/" + encodeURIComponent("2024-01-01T00:00:00.000Z_31622400000_0");

        const member = members[0];
        member.timestamp = new Date(member.timestamp);
        member.timestamp.setUTCFullYear(2024);

        const recordBuckets = orchestrator.bucketize(
            memberToRecord(member),
            buckets,
            new Map<string, Set<Term>>(),
            new Map<string, Set<string>>(),
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(firstBucketExpected);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);
    });
});
