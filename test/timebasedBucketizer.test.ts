import { beforeEach, describe, expect, test } from "vitest";
import { DataFactory, Parser } from "n3";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    SHAPES_TEXT,
} from "../lib/bucketizers/index";
import { Bucket, Bucketizer, Record } from "../lib/";
import { extractShapes, pred } from "rdf-lens";
import { FullProc, ReaderInstance, WriterInstance } from "@rdfc/js-runner";
import { RDF, SDS } from "@treecg/types";
import {
    createReader,
    createWriter,
    logger,
    one,
} from "@rdfc/js-runner/lib/testUtils";
const { namedNode } = DataFactory;

type Member = { id: string; timestamp: Date; text: string };

describe("TimebasedBucketizer tests", () => {
    const members: { id: string; timestamp: Date; text: string }[] = [];
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

        const firstMemberId = `abcd-beginning-of-${firstOfMonth.toLocaleString(
            "default",
            { month: "long" },
        )}-${firstOfMonth.getFullYear()}-efgh`;
        const secondMemberId = `ijkl-middle-of-${seventeenthOfMonth.toLocaleString(
            "default",
            { month: "long" },
        )}-${seventeenthOfMonth.getFullYear()}-mnop`;

        members.push({
            id: firstMemberId,
            timestamp: firstOfMonth,
            text: `This is a member that was added at the beginning of ${firstOfMonth.toLocaleString(
                "default",
                { month: "long" },
            )} ${firstOfMonth.getFullYear()}`,
        });

        members.push({
            id: secondMemberId,
            timestamp: seventeenthOfMonth,
            text: `This is a member that was added in the middle of ${seventeenthOfMonth.toLocaleString(
                "default",
                { month: "long" },
            )} ${seventeenthOfMonth.getFullYear()}`,
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

    test("bucketize with (k = 4, m = 10, s = 3600) should split the bucket", () => {
        const orchestrator = getOrchestrator(4, 10, 3600);

        const firstBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";

        const secondSplitBucketExpected =
            encodeURIComponent("2023-04-02T06:00:00.000Z_7884000000_0") + "/";
        const thirdSplitBucketExpected =
            encodeURIComponent("2023-07-02T12:00:00.000Z_7884000000_0") + "/";
        const fourthSplitBucketExpected =
            encodeURIComponent("2023-10-01T18:00:00.000Z_7884000000_0") + "/";

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";

        // All the members should fit in the first bucket.
        for (let i = 0; i < 24; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets[""].root).toBeTruthy();
            expect(buckets[""].links.length).toBe(2);
        }
    });

    test("bucketize with (k = 4, m = 100, s = 3600) and prefix root/ should all add to the first bucket", () => {
        const orchestrator = getOrchestrator(4, 100, 3600);

        const firstBucketExpected =
            "root/" +
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") +
            "/";

        // All the members should fit in the first bucket.
        for (let i = 0; i < 24; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "root/",
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets["root/"].root).toBeTruthy();
            expect(buckets["root/"].links.length).toBe(2);
        }
    });

    test("bucketize with (k = 4, m = 10, s = 30000000000) should make new pages", () => {
        const orchestrator = getOrchestrator(4, 10, 30000000000);

        const firstBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";
        const secondBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_1") + "/";
        const thirdBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_2") + "/";

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(thirdBucketExpected);
            expect(buckets[secondBucketExpected].root).toBeFalsy();
            expect(buckets[secondBucketExpected].links.length).toBe(1);
        }
    });

    test("bucketize with (k = 4, m = 10, s = 30000000000) and prefix root/ should make new pages", () => {
        const orchestrator = getOrchestrator(4, 10, 30000000000);

        const firstBucketExpected =
            "root/" +
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") +
            "/";
        const secondBucketExpected =
            "root/" +
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_1") +
            "/";
        const thirdBucketExpected =
            "root/" +
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_2") +
            "/";

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "root/",
            );
            expect(recordBuckets.length).toBe(1);
            expect(recordBuckets[0]).toBe(firstBucketExpected);
            expect(buckets["root/"].root).toBeTruthy();
            expect(buckets["root/"].links.length).toBe(2);
        }

        // The next 10 member should be added to a new page bucket, as we cannot split due to timespan constraints.
        for (let i = 10; i < 20; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "root/",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "root/",
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
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";

        const secondSplitBucketExpected =
            encodeURIComponent("2023-04-02T06:00:00.000Z_7884000000_0") + "/";
        const thirdSplitBucketExpected =
            encodeURIComponent("2023-07-02T12:00:00.000Z_7884000000_0") + "/";
        const fourthSplitBucketExpected =
            encodeURIComponent("2023-10-01T18:00:00.000Z_7884000000_0") + "/";

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";

        // Add first 10 members, should all be added to the first single bucket.
        for (let i = 0; i < 10; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";
        const secondBucketExpected =
            encodeURIComponent("2023-02-15T15:00:00.000Z_3942000000_0") + "/";

        // First 3 members should fit in the first bucket.
        for (let i = 0; i < 3; i++) {
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(members[i]),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
            new Set(),
            new Map<string, Set<string>>(),
            [],
            [],
            "",
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(secondBucketExpected);
    });

    test("bucketize with (k = 2, m = 3, s = 3600) should recursively split the buckets a lot until it needs to make a new page", () => {
        const orchestrator = getOrchestrator(2, 3, 3600);

        const firstBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_31536000000_0") + "/";
        const secondBucketExpected =
            encodeURIComponent("2023-01-01T00:00:00.000Z_3849610_1") + "/";

        // First 3 members should fit in the first bucket.
        for (let i = 0; i < 3; i++) {
            const member = members[0];
            member.timestamp = new Date(member.timestamp);
            member.timestamp.setUTCMinutes(i);
            const recordBuckets = orchestrator.bucketize(
                memberToRecord(member),
                buckets,
                new Set(),
                new Map<string, Set<string>>(),
                [],
                [],
                "",
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
            new Set(),
            new Map<string, Set<string>>(),
            [],
            [],
            "",
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(secondBucketExpected);
        expect(buckets[secondBucketExpected].parent!.links.length).toBe(1);
    });

    test("bucketize with (k = 4, m = 100, s = 3600) should add to a first bucket with adjusted timespan for leap year", () => {
        const orchestrator = getOrchestrator(4, 100, 3600);

        const firstBucketExpected =
            encodeURIComponent("2024-01-01T00:00:00.000Z_31622400000_0") + "/";

        const member = members[0];
        member.timestamp = new Date(member.timestamp);
        member.timestamp.setUTCFullYear(2024);

        const recordBuckets = orchestrator.bucketize(
            memberToRecord(member),
            buckets,
            new Set(),
            new Map<string, Set<string>>(),
            [],
            [],
            "",
        );
        expect(recordBuckets.length).toBe(1);
        expect(recordBuckets[0]).toBe(firstBucketExpected);
        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);
    });

    test("realistic test with (k = 4, m = 10, s = 3600) and 15 records timestamped every day", async () => {
        /**
         * This test is a realistic test where we have 15 records timestamped every day.
         * Records start from 2024-07-23 and end at 2024-08-18.
         * The 15 records start at 3am-2 and are separated by 5 seconds.
         * The bucketizer is configured with k = 4, m = 10, s = 3600.
         * We add the records in batches per day and check if the records are added to the correct buckets.
         */
        // START SETUP
        const [incomingWriter, incoming] = createWriter("incoming");
        const [outgoing, outgoingReader] = createWriter("outgoing");

        // Initialize the processor.
        await setupBucketizer(incoming, outgoing, 10, 4, 3600);

        let output: string[] = [];
        // [[1...15 for 2024-07-23], [1...15 for 2024-07-24], ..., [1...15 for 2024-08-18]]; with 1 at 3:00:00-2, 2 at 3:00:05-2, 3 at 3:00:10-2, ...
        const dates: Date[][] = [];
        const start = new Date("2024-07-23T01:00:00Z");
        for (let i = 0; i < 26; i++) {
            const startAtDate = new Date(start.getTime() + i * 86400000);
            const datesAtDate: Date[] = [];
            for (let j = 0; j < 15; j++) {
                datesAtDate.push(new Date(startAtDate.getTime() + j * 5000));
            }
            dates.push(datesAtDate);
        }
        // END SETUP

        // START TEST
        const b0 = new Date("2024-01-01T00:00:00Z");
        const b1 = new Date("2025-01-01T00:00:00Z");

        // First 10 should just be added
        for (let i = 0; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`ra${i + 1}`, dates[0][i]));
            await outputPromise;

            const expected = [
                {
                    id: `${encodeURIComponent(b0.toISOString())}_${ts(b0, b1)}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ];
            if (i == 0) {
                expected.unshift({
                    id: "",
                    immutable: false,
                    relations: 2,
                });
            }
            testOutput(
                output,
                `${encodeURIComponent(b0.toISOString())}_${ts(b0, b1)}_0/`,
                [],
                expected,
                [
                    {
                        id: `ra${i + 1}`,
                        bucket: `${encodeURIComponent(b0.toISOString())}_${ts(
                            b0,
                            b1,
                        )}_0/`,
                    },
                ],
            );
        }

        // Ra11 should split the bucket recursively until it has to make a new page.
        output = [];
        let outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("ra11", dates[0][10]));
        await outputPromise;

        // Expected splits
        const [b0_0, b0_1, b0_2, b0_3, b0_4] = splitTimespan(b0, b1, 4);
        const [b0_2_0, b0_2_1, b0_2_2, b0_2_3, b0_2_4] = splitTimespan(
            b0_2,
            b0_3,
            4,
        );
        const [b0_2_0_0, b0_2_0_1, b0_2_0_2, b0_2_0_3, b0_2_0_4] =
            splitTimespan(b0_2_0, b0_2_1, 4);
        const [b0_2_0_3_0, b0_2_0_3_1, b0_2_0_3_2, b0_2_0_3_3, b0_2_0_3_4] =
            splitTimespan(b0_2_0_3, b0_2_0_4, 4);
        const [
            b0_2_0_3_2_0,
            b0_2_0_3_2_1,
            b0_2_0_3_2_2,
            b0_2_0_3_2_3,
            b0_2_0_3_2_4,
        ] = splitTimespan(b0_2_0_3_2, b0_2_0_3_3, 4);
        const [
            b0_2_0_3_2_2_0,
            b0_2_0_3_2_2_1,
            b0_2_0_3_2_2_2,
            b0_2_0_3_2_2_3,
            b0_2_0_3_2_2_4,
        ] = splitTimespan(b0_2_0_3_2_2, b0_2_0_3_2_3, 4);

        //                                                                                                                              Expected new page
        testOutput(
            output,
            `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                b0_2_0_3_2_2_3,
                b0_2_0_3_2_2_4,
            )}_1/`,
            [
                `${encodeURIComponent(b0.toISOString())}_${ts(b0, b1)}_0/`,
                `${encodeURIComponent(b0_2.toISOString())}_${ts(b0_2, b0_3)}_0/`,
                `${encodeURIComponent(b0_2_0.toISOString())}_${ts(
                    b0_2_0,
                    b0_2_1,
                )}_0/`,
                `${encodeURIComponent(b0_2_0_3.toISOString())}_${ts(
                    b0_2_0_3,
                    b0_2_0_4,
                )}_0/`,
                `${encodeURIComponent(b0_2_0_3_2.toISOString())}_${ts(
                    b0_2_0_3_2,
                    b0_2_0_3_3,
                )}_0/`,
                `${encodeURIComponent(b0_2_0_3_2_2.toISOString())}_${ts(
                    b0_2_0_3_2_2,
                    b0_2_0_3_2_3,
                )}_0/`,
            ],
            [
                // { id: "root", immutable: false, relations: 2 },
                {
                    id: `${encodeURIComponent(b0.toISOString())}_${ts(b0, b1)}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_0.toISOString())}_${ts(
                        b0_0,
                        b0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_1.toISOString())}_${ts(
                        b0_1,
                        b0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2.toISOString())}_${ts(
                        b0_2,
                        b0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_3.toISOString())}_${ts(
                        b0_3,
                        b0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0.toISOString())}_${ts(
                        b0_2_0,
                        b0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                        b0_2_1,
                        b0_2_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_2.toISOString())}_${ts(
                        b0_2_2,
                        b0_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_3.toISOString())}_${ts(
                        b0_2_3,
                        b0_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_0.toISOString())}_${ts(
                        b0_2_0_0,
                        b0_2_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_1.toISOString())}_${ts(
                        b0_2_0_1,
                        b0_2_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_2.toISOString())}_${ts(
                        b0_2_0_2,
                        b0_2_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3.toISOString())}_${ts(
                        b0_2_0_3,
                        b0_2_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_0.toISOString())}_${ts(
                        b0_2_0_3_0,
                        b0_2_0_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_1.toISOString())}_${ts(
                        b0_2_0_3_1,
                        b0_2_0_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2.toISOString())}_${ts(
                        b0_2_0_3_2,
                        b0_2_0_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                        b0_2_0_3_3,
                        b0_2_0_3_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_0.toISOString())}_${ts(
                        b0_2_0_3_2_0,
                        b0_2_0_3_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_1.toISOString())}_${ts(
                        b0_2_0_3_2_1,
                        b0_2_0_3_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2.toISOString())}_${ts(
                        b0_2_0_3_2_2,
                        b0_2_0_3_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_3,
                        b0_2_0_3_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_0.toISOString())}_${ts(
                        b0_2_0_3_2_2_0,
                        b0_2_0_3_2_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_1.toISOString())}_${ts(
                        b0_2_0_3_2_2_1,
                        b0_2_0_3_2_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_2.toISOString())}_${ts(
                        b0_2_0_3_2_2_2,
                        b0_2_0_3_2_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 1,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_1/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ra1",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra2",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra3",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra4",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra5",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra6",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra7",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra8",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra9",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra10",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                },
                {
                    id: "ra11",
                    bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_1/`,
                },
            ],
        );

        // Ra12 - Ra15 should be added to the new page
        for (let i = 11; i < 15; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`ra${i + 1}`, dates[0][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                    b0_2_0_3_2_2_3,
                    b0_2_0_3_2_2_4,
                )}_1/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                            b0_2_0_3_2_2_3,
                            b0_2_0_3_2_2_4,
                        )}_1/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `ra${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                            b0_2_0_3_2_2_3,
                            b0_2_0_3_2_2_4,
                        )}_1/`,
                    },
                ],
            );
        }

        // Next day (2024-07-24), Rb1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rb1", dates[1][0]));
        await outputPromise;

        testOutput(
            output,
            `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                b0_2_0_3_3,
                b0_2_0_3_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_0_3_0.toISOString())}_${ts(
                        b0_2_0_3_0,
                        b0_2_0_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_1.toISOString())}_${ts(
                        b0_2_0_3_1,
                        b0_2_0_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2.toISOString())}_${ts(
                        b0_2_0_3_2,
                        b0_2_0_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                        b0_2_0_3_3,
                        b0_2_0_3_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_0.toISOString())}_${ts(
                        b0_2_0_3_2_0,
                        b0_2_0_3_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_1.toISOString())}_${ts(
                        b0_2_0_3_2_1,
                        b0_2_0_3_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2.toISOString())}_${ts(
                        b0_2_0_3_2_2,
                        b0_2_0_3_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_3,
                        b0_2_0_3_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_0.toISOString())}_${ts(
                        b0_2_0_3_2_2_0,
                        b0_2_0_3_2_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_1.toISOString())}_${ts(
                        b0_2_0_3_2_2_1,
                        b0_2_0_3_2_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_2.toISOString())}_${ts(
                        b0_2_0_3_2_2_2,
                        b0_2_0_3_2_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2_2_3.toISOString())}_${ts(
                        b0_2_0_3_2_2_3,
                        b0_2_0_3_2_2_4,
                    )}_1/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rb1",
                    bucket: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                        b0_2_0_3_3,
                        b0_2_0_3_4,
                    )}_0/`,
                },
            ],
        );

        // Rb2 - Rb10 should just be added.
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rb${i + 1}`, dates[1][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                    b0_2_0_3_3,
                    b0_2_0_3_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                            b0_2_0_3_3,
                            b0_2_0_3_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rb${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                            b0_2_0_3_3,
                            b0_2_0_3_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Rb11 should split the bucket recursively until it has to make a new page.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rb11", dates[1][10]));
        await outputPromise;

        // Expected splits
        const [
            b0_2_0_3_3_0,
            b0_2_0_3_3_1,
            b0_2_0_3_3_2,
            b0_2_0_3_3_3,
            b0_2_0_3_3_4,
        ] = splitTimespan(b0_2_0_3_3, b0_2_0_3_4, 4);
        const [
            b0_2_0_3_3_1_0,
            b0_2_0_3_3_1_1,
            b0_2_0_3_3_1_2,
            b0_2_0_3_3_1_3,
            b0_2_0_3_3_1_4,
        ] = splitTimespan(b0_2_0_3_3_1, b0_2_0_3_3_2, 4);

        //                                                                                                                              Expected new page
        testOutput(
            output,
            `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                b0_2_0_3_3_1_2,
                b0_2_0_3_3_1_3,
            )}_1/`,
            [
                `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                    b0_2_0_3_3,
                    b0_2_0_3_4,
                )}_0/`,
                `${encodeURIComponent(b0_2_0_3_3_1.toISOString())}_${ts(
                    b0_2_0_3_3_1,
                    b0_2_0_3_3_2,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                        b0_2_0_3_3,
                        b0_2_0_3_4,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_0.toISOString())}_${ts(
                        b0_2_0_3_3_0,
                        b0_2_0_3_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1.toISOString())}_${ts(
                        b0_2_0_3_3_1,
                        b0_2_0_3_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_2.toISOString())}_${ts(
                        b0_2_0_3_3_2,
                        b0_2_0_3_3_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_3.toISOString())}_${ts(
                        b0_2_0_3_3_3,
                        b0_2_0_3_3_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_0.toISOString())}_${ts(
                        b0_2_0_3_3_1_0,
                        b0_2_0_3_3_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_1.toISOString())}_${ts(
                        b0_2_0_3_3_1_1,
                        b0_2_0_3_3_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 1,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_1/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_3.toISOString())}_${ts(
                        b0_2_0_3_3_1_3,
                        b0_2_0_3_3_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rb1",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb2",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb3",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb4",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb5",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb6",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb7",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb8",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb9",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb10",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                },
                {
                    id: "rb11",
                    bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_1/`,
                },
            ],
        );

        // Rb12 - Rb15 should be added to the new page
        for (let i = 11; i < 15; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rb${i + 1}`, dates[1][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                    b0_2_0_3_3_1_2,
                    b0_2_0_3_3_1_3,
                )}_1/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                            b0_2_0_3_3_1_2,
                            b0_2_0_3_3_1_3,
                        )}_1/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rb${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                            b0_2_0_3_3_1_2,
                            b0_2_0_3_3_1_3,
                        )}_1/`,
                    },
                ],
            );
        }

        // Next day (2024-07-25), Rc1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rc1", dates[2][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                b0_2_1,
                b0_2_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_0.toISOString())}_${ts(
                        b0_2_0,
                        b0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                        b0_2_1,
                        b0_2_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_2.toISOString())}_${ts(
                        b0_2_2,
                        b0_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_3.toISOString())}_${ts(
                        b0_2_3,
                        b0_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_0.toISOString())}_${ts(
                        b0_2_0_0,
                        b0_2_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_1.toISOString())}_${ts(
                        b0_2_0_1,
                        b0_2_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_2.toISOString())}_${ts(
                        b0_2_0_2,
                        b0_2_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3.toISOString())}_${ts(
                        b0_2_0_3,
                        b0_2_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_0.toISOString())}_${ts(
                        b0_2_0_3_0,
                        b0_2_0_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_1.toISOString())}_${ts(
                        b0_2_0_3_1,
                        b0_2_0_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_2.toISOString())}_${ts(
                        b0_2_0_3_2,
                        b0_2_0_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3.toISOString())}_${ts(
                        b0_2_0_3_3,
                        b0_2_0_3_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_0.toISOString())}_${ts(
                        b0_2_0_3_3_0,
                        b0_2_0_3_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1.toISOString())}_${ts(
                        b0_2_0_3_3_1,
                        b0_2_0_3_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_2.toISOString())}_${ts(
                        b0_2_0_3_3_2,
                        b0_2_0_3_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_3.toISOString())}_${ts(
                        b0_2_0_3_3_3,
                        b0_2_0_3_3_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_0.toISOString())}_${ts(
                        b0_2_0_3_3_1_0,
                        b0_2_0_3_3_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_1.toISOString())}_${ts(
                        b0_2_0_3_3_1_1,
                        b0_2_0_3_3_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_2.toISOString())}_${ts(
                        b0_2_0_3_3_1_2,
                        b0_2_0_3_3_1_3,
                    )}_1/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_0_3_3_1_3.toISOString())}_${ts(
                        b0_2_0_3_3_1_3,
                        b0_2_0_3_3_1_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rc1",
                    bucket: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                        b0_2_1,
                        b0_2_2,
                    )}_0/`,
                },
            ],
        );

        // Rc2 - Rc10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rc${i + 1}`, dates[2][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                    b0_2_1,
                    b0_2_2,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                            b0_2_1,
                            b0_2_2,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rc${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                            b0_2_1,
                            b0_2_2,
                        )}_0/`,
                    },
                ],
            );
        }

        // Rc11 should split the bucket recursively until it has to make a new page.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rc11", dates[2][10]));
        await outputPromise;

        // Expected splits
        const [b0_2_1_0, b0_2_1_1, b0_2_1_2, b0_2_1_3, b0_2_1_4] =
            splitTimespan(b0_2_1, b0_2_2, 4);
        const [b0_2_1_0_0, b0_2_1_0_1, b0_2_1_0_2, b0_2_1_0_3, b0_2_1_0_4] =
            splitTimespan(b0_2_1_0, b0_2_1_1, 4);
        const [
            b0_2_1_0_0_0,
            b0_2_1_0_0_1,
            b0_2_1_0_0_2,
            b0_2_1_0_0_3,
            b0_2_1_0_0_4,
        ] = splitTimespan(b0_2_1_0_0, b0_2_1_0_1, 4);
        const [
            b0_2_1_0_0_0_0,
            b0_2_1_0_0_0_1,
            b0_2_1_0_0_0_2,
            b0_2_1_0_0_0_3,
            b0_2_1_0_0_0_4,
        ] = splitTimespan(b0_2_1_0_0_0, b0_2_1_0_0_1, 4);

        //                                                                                                                              Expected new page
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                b0_2_1_0_0_0_1,
                b0_2_1_0_0_0_2,
            )}_1/`,
            [
                `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                    b0_2_1,
                    b0_2_2,
                )}_0/`,
                `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                    b0_2_1_0,
                    b0_2_1_1,
                )}_0/`,
                `${encodeURIComponent(b0_2_1_0_0.toISOString())}_${ts(
                    b0_2_1_0_0,
                    b0_2_1_0_1,
                )}_0/`,
                `${encodeURIComponent(b0_2_1_0_0_0.toISOString())}_${ts(
                    b0_2_1_0_0_0,
                    b0_2_1_0_0_1,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                        b0_2_1,
                        b0_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                        b0_2_1_0,
                        b0_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0.toISOString())}_${ts(
                        b0_2_1_0_0,
                        b0_2_1_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                        b0_2_1_0_1,
                        b0_2_1_0_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_3.toISOString())}_${ts(
                        b0_2_1_0_3,
                        b0_2_1_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0.toISOString())}_${ts(
                        b0_2_1_0_0_0,
                        b0_2_1_0_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_1,
                        b0_2_1_0_0_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_2.toISOString())}_${ts(
                        b0_2_1_0_0_2,
                        b0_2_1_0_0_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_3,
                        b0_2_1_0_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_0.toISOString())}_${ts(
                        b0_2_1_0_0_0_0,
                        b0_2_1_0_0_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 1,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_1/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_2.toISOString())}_${ts(
                        b0_2_1_0_0_0_2,
                        b0_2_1_0_0_0_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_0_3,
                        b0_2_1_0_0_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rc1",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc2",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc3",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc4",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc5",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc6",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc7",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc8",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc9",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc10",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                },
                {
                    id: "rc11",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_1/`,
                },
            ],
        );

        // Rc12 - Rc15 should be added to the new page
        for (let i = 11; i < 15; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rc${i + 1}`, dates[2][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                    b0_2_1_0_0_0_1,
                    b0_2_1_0_0_0_2,
                )}_1/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                            b0_2_1_0_0_0_1,
                            b0_2_1_0_0_0_2,
                        )}_1/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rc${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                            b0_2_1_0_0_0_1,
                            b0_2_1_0_0_0_2,
                        )}_1/`,
                    },
                ],
            );
        }

        // Next day (2024-07-26), Rd1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rd1", dates[3][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                b0_2_1_0_0_3,
                b0_2_1_0_0_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0.toISOString())}_${ts(
                        b0_2_1_0_0_0,
                        b0_2_1_0_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_1,
                        b0_2_1_0_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_2.toISOString())}_${ts(
                        b0_2_1_0_0_2,
                        b0_2_1_0_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_3,
                        b0_2_1_0_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_0.toISOString())}_${ts(
                        b0_2_1_0_0_0_0,
                        b0_2_1_0_0_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_0_1,
                        b0_2_1_0_0_0_2,
                    )}_1/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_2.toISOString())}_${ts(
                        b0_2_1_0_0_0_2,
                        b0_2_1_0_0_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_0_3,
                        b0_2_1_0_0_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rd1",
                    bucket: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_3,
                        b0_2_1_0_0_4,
                    )}_0/`,
                },
            ],
        );

        // Rd2 - Rd10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rd${i + 1}`, dates[3][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                    b0_2_1_0_0_3,
                    b0_2_1_0_0_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                            b0_2_1_0_0_3,
                            b0_2_1_0_0_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rd${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                            b0_2_1_0_0_3,
                            b0_2_1_0_0_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rd11 - Rd15.

        // Next day (2024-07-27), Re1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("re1", dates[4][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                b0_2_1_0_1,
                b0_2_1_0_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0_0.toISOString())}_${ts(
                        b0_2_1_0_0,
                        b0_2_1_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                        b0_2_1_0_1,
                        b0_2_1_0_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_3.toISOString())}_${ts(
                        b0_2_1_0_3,
                        b0_2_1_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_0.toISOString())}_${ts(
                        b0_2_1_0_0_0,
                        b0_2_1_0_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_1.toISOString())}_${ts(
                        b0_2_1_0_0_1,
                        b0_2_1_0_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_2.toISOString())}_${ts(
                        b0_2_1_0_0_2,
                        b0_2_1_0_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0_3.toISOString())}_${ts(
                        b0_2_1_0_0_3,
                        b0_2_1_0_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "re1",
                    bucket: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                        b0_2_1_0_1,
                        b0_2_1_0_2,
                    )}_0/`,
                },
            ],
        );

        // Re2 - Re10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`re${i + 1}`, dates[4][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                    b0_2_1_0_1,
                    b0_2_1_0_2,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                            b0_2_1_0_1,
                            b0_2_1_0_2,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `re${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                            b0_2_1_0_1,
                            b0_2_1_0_2,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Re11 - Re15.

        // Next day (2024-07-28), Rf1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rf1", dates[5][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                b0_2_1_0_2,
                b0_2_1_0_3,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0_0.toISOString())}_${ts(
                        b0_2_1_0_0,
                        b0_2_1_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                        b0_2_1_0_1,
                        b0_2_1_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_3.toISOString())}_${ts(
                        b0_2_1_0_3,
                        b0_2_1_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rf1",
                    bucket: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                },
            ],
        );

        // Rf2 - Rf10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rf${i + 1}`, dates[5][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                    b0_2_1_0_2,
                    b0_2_1_0_3,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                            b0_2_1_0_2,
                            b0_2_1_0_3,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rf${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                            b0_2_1_0_2,
                            b0_2_1_0_3,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rf11 - Rf15.

        // Next day (2024-07-29), Rg1 should be added to same bucket as Rf1-Rf10, splitting that bucket.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rg1", dates[6][0]));
        await outputPromise;

        // Expected splits
        const [
            b0_2_1_0_2_0,
            b0_2_1_0_2_1,
            b0_2_1_0_2_2,
            b0_2_1_0_2_3,
            b0_2_1_0_2_4,
        ] = splitTimespan(b0_2_1_0_2, b0_2_1_0_3, 4);

        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                b0_2_1_0_2_3,
                b0_2_1_0_2_4,
            )}_0/`,
            [
                `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                    b0_2_1_0_2,
                    b0_2_1_0_3,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_1.toISOString())}_${ts(
                        b0_2_1_0_2_1,
                        b0_2_1_0_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_2.toISOString())}_${ts(
                        b0_2_1_0_2_2,
                        b0_2_1_0_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                        b0_2_1_0_2_3,
                        b0_2_1_0_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rf1",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf2",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf3",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf4",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf5",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf6",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf7",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf8",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf9",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rf10",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rg1",
                    bucket: `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                        b0_2_1_0_2_3,
                        b0_2_1_0_2_4,
                    )}_0/`,
                },
            ],
        );

        // Rg2 - Rg10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rg${i + 1}`, dates[6][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                    b0_2_1_0_2_3,
                    b0_2_1_0_2_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                            b0_2_1_0_2_3,
                            b0_2_1_0_2_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rg${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                            b0_2_1_0_2_3,
                            b0_2_1_0_2_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rg11 - Rg15.

        // Skip adding 2024-07-30.

        // Next day (2024-07-31), Rh1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rh1", dates[8][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                        b0_2_1_0,
                        b0_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_0.toISOString())}_${ts(
                        b0_2_1_0_0,
                        b0_2_1_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_1.toISOString())}_${ts(
                        b0_2_1_0_1,
                        b0_2_1_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2.toISOString())}_${ts(
                        b0_2_1_0_2,
                        b0_2_1_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_3.toISOString())}_${ts(
                        b0_2_1_0_3,
                        b0_2_1_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_0.toISOString())}_${ts(
                        b0_2_1_0_2_0,
                        b0_2_1_0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_1.toISOString())}_${ts(
                        b0_2_1_0_2_1,
                        b0_2_1_0_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_2.toISOString())}_${ts(
                        b0_2_1_0_2_2,
                        b0_2_1_0_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0_2_3.toISOString())}_${ts(
                        b0_2_1_0_2_3,
                        b0_2_1_0_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rh1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Rh2 - Rh5 should just be added
        for (let i = 1; i < 5; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(createInput(`rh${i + 1}`, dates[8][i]));
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                    b0_2_1_1,
                    b0_2_1_2,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                            b0_2_1_1,
                            b0_2_1_2,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rh${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                            b0_2_1_1,
                            b0_2_1_2,
                        )}_0/`,
                    },
                ],
            );
        }

        // Next day (2024-08-01), Ri1 should be added to same bucket as Rh1-Rh5.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("ri1", dates[9][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ri1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Next day (2024-08-02), Rj1 should be added to same bucket as Ri1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rj1", dates[10][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rj1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Next day (2024-08-03), Rk1 should be added to same bucket as Rj1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rk1", dates[11][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rk1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Next day (2024-08-04), Rl1 should be added to same bucket as Rk1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rl1", dates[12][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rl1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Next day (2024-08-05), Rm1 should be added to same bucket as Rl1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rm1", dates[13][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                b0_2_1_1,
                b0_2_1_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rm1",
                    bucket: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                },
            ],
        );

        // Next day (2024-08-06), Rn1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rn1", dates[14][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                b0_2_1_2,
                b0_2_1_3,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                        b0_2_1_0,
                        b0_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rn1",
                    bucket: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                },
            ],
        );

        // Rn2 - Rn10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rn${i + 1}`, dates[14][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                    b0_2_1_2,
                    b0_2_1_3,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                            b0_2_1_2,
                            b0_2_1_3,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rn${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                            b0_2_1_2,
                            b0_2_1_3,
                        )}_0/`,
                    },
                ],
            );
        }

        // Rn11 should split the bucket recursively until it has to make a new page.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rn11", dates[14][10]));
        await outputPromise;

        // Expected splits
        const [b0_2_1_2_0, b0_2_1_2_1, b0_2_1_2_2, b0_2_1_2_3, b0_2_1_2_4] =
            splitTimespan(b0_2_1_2, b0_2_1_3, 4);
        const [
            b0_2_1_2_0_0,
            b0_2_1_2_0_1,
            b0_2_1_2_0_2,
            b0_2_1_2_0_3,
            b0_2_1_2_0_4,
        ] = splitTimespan(b0_2_1_2_0, b0_2_1_2_1, 4);
        const [
            b0_2_1_2_0_2_0,
            b0_2_1_2_0_2_1,
            b0_2_1_2_0_2_2,
            b0_2_1_2_0_2_3,
            b0_2_1_2_0_2_4,
        ] = splitTimespan(b0_2_1_2_0_2, b0_2_1_2_0_3, 4);

        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                b0_2_1_2_0_2_0,
                b0_2_1_2_0_2_1,
            )}_1/`,
            [
                `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                    b0_2_1_2,
                    b0_2_1_3,
                )}_0/`,
                `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                    b0_2_1_2_0,
                    b0_2_1_2_1,
                )}_0/`,
                `${encodeURIComponent(b0_2_1_2_0_2.toISOString())}_${ts(
                    b0_2_1_2_0_2,
                    b0_2_1_2_0_3,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                        b0_2_1_2_0,
                        b0_2_1_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_0.toISOString())}_${ts(
                        b0_2_1_2_0_0,
                        b0_2_1_2_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_1.toISOString())}_${ts(
                        b0_2_1_2_0_1,
                        b0_2_1_2_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2.toISOString())}_${ts(
                        b0_2_1_2_0_2,
                        b0_2_1_2_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_3.toISOString())}_${ts(
                        b0_2_1_2_0_3,
                        b0_2_1_2_0_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 1,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_1/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_1.toISOString())}_${ts(
                        b0_2_1_2_0_2_1,
                        b0_2_1_2_0_2_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_2.toISOString())}_${ts(
                        b0_2_1_2_0_2_2,
                        b0_2_1_2_0_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_3.toISOString())}_${ts(
                        b0_2_1_2_0_2_3,
                        b0_2_1_2_0_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rn1",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn2",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn3",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn4",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn5",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn6",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn7",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn8",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn9",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn10",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                },
                {
                    id: "rn11",
                    bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_1/`,
                },
            ],
        );

        // Rn12 - Rn15 should be added to the new page.
        for (let i = 1; i < 5; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rn${i + 11}`, dates[14][10 + i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                    b0_2_1_2_0_2_0,
                    b0_2_1_2_0_2_1,
                )}_1/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                            b0_2_1_2_0_2_0,
                            b0_2_1_2_0_2_1,
                        )}_1/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rn${i + 11}`,
                        bucket: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                            b0_2_1_2_0_2_0,
                            b0_2_1_2_0_2_1,
                        )}_1/`,
                    },
                ],
            );
        }

        // Next day (2024-08-07), Ro1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("ro1", dates[15][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                b0_2_1_2_1,
                b0_2_1_2_2,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                        b0_2_1_2_0,
                        b0_2_1_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_0.toISOString())}_${ts(
                        b0_2_1_2_0_0,
                        b0_2_1_2_0_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_1.toISOString())}_${ts(
                        b0_2_1_2_0_1,
                        b0_2_1_2_0_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2.toISOString())}_${ts(
                        b0_2_1_2_0_2,
                        b0_2_1_2_0_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_3.toISOString())}_${ts(
                        b0_2_1_2_0_3,
                        b0_2_1_2_0_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_0.toISOString())}_${ts(
                        b0_2_1_2_0_2_0,
                        b0_2_1_2_0_2_1,
                    )}_1/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_1.toISOString())}_${ts(
                        b0_2_1_2_0_2_1,
                        b0_2_1_2_0_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_2.toISOString())}_${ts(
                        b0_2_1_2_0_2_2,
                        b0_2_1_2_0_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0_2_3.toISOString())}_${ts(
                        b0_2_1_2_0_2_3,
                        b0_2_1_2_0_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ro1",
                    bucket: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                },
            ],
        );

        // Ro2 - Ro10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`ro${i + 1}`, dates[15][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                    b0_2_1_2_1,
                    b0_2_1_2_2,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                            b0_2_1_2_1,
                            b0_2_1_2_2,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `ro${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                            b0_2_1_2_1,
                            b0_2_1_2_2,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Ro11 - Ro15

        // Next day (2024-08-08), Rp1 should be added, recursively splitting the bucket Ro1 - Ro10 also belongs to.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rp1", dates[16][0]));
        await outputPromise;

        // Expected splits
        const [
            b0_2_1_2_1_0,
            b0_2_1_2_1_1,
            b0_2_1_2_1_2,
            b0_2_1_2_1_3,
            b0_2_1_2_1_4,
        ] = splitTimespan(b0_2_1_2_1, b0_2_1_2_2, 4);

        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                b0_2_1_2_1_3,
                b0_2_1_2_1_4,
            )}_0/`,
            [
                `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                    b0_2_1_2_1,
                    b0_2_1_2_2,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_1.toISOString())}_${ts(
                        b0_2_1_2_1_1,
                        b0_2_1_2_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_2.toISOString())}_${ts(
                        b0_2_1_2_1_2,
                        b0_2_1_2_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                        b0_2_1_2_1_3,
                        b0_2_1_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ro1",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro2",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro3",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro4",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro5",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro6",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro7",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro8",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro9",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "ro10",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                },
                {
                    id: "rp1",
                    bucket: `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                        b0_2_1_2_1_3,
                        b0_2_1_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Rp2 - Rp10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rp${i + 1}`, dates[16][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                    b0_2_1_2_1_3,
                    b0_2_1_2_1_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                            b0_2_1_2_1_3,
                            b0_2_1_2_1_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rp${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                            b0_2_1_2_1_3,
                            b0_2_1_2_1_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rp11 - Rp15

        // Next day (2024-08-09), Rq1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rq1", dates[17][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                b0_2_1_2_2,
                b0_2_1_2_3,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                        b0_2_1_2_0,
                        b0_2_1_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_0.toISOString())}_${ts(
                        b0_2_1_2_1_0,
                        b0_2_1_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_1.toISOString())}_${ts(
                        b0_2_1_2_1_1,
                        b0_2_1_2_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_2.toISOString())}_${ts(
                        b0_2_1_2_1_2,
                        b0_2_1_2_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1_3.toISOString())}_${ts(
                        b0_2_1_2_1_3,
                        b0_2_1_2_1_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rq1",
                    bucket: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                },
            ],
        );

        // Rq2 - Rq10 should just be added
        for (let i = 1; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rq${i + 1}`, dates[17][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                    b0_2_1_2_2,
                    b0_2_1_2_3,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                            b0_2_1_2_2,
                            b0_2_1_2_3,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rq${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                            b0_2_1_2_2,
                            b0_2_1_2_3,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rq11 - Rq15

        // Next day (2024-08-10), Rr1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rr1", dates[18][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                b0_2_1_2_3,
                b0_2_1_2_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                        b0_2_1_2_0,
                        b0_2_1_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rr1",
                    bucket: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                },
            ],
        );

        // Rr2 - Rr5 should just be added
        for (let i = 1; i < 5; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rr${i + 1}`, dates[18][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                    b0_2_1_2_3,
                    b0_2_1_2_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                            b0_2_1_2_3,
                            b0_2_1_2_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rr${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                            b0_2_1_2_3,
                            b0_2_1_2_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rr6 - Rr15

        // Next day (2024-08-11), Rs1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rs1", dates[19][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                b0_2_1_3,
                b0_2_1_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                        b0_2_1_0,
                        b0_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_0.toISOString())}_${ts(
                        b0_2_1_2_0,
                        b0_2_1_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_1.toISOString())}_${ts(
                        b0_2_1_2_1,
                        b0_2_1_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_2.toISOString())}_${ts(
                        b0_2_1_2_2,
                        b0_2_1_2_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2_3.toISOString())}_${ts(
                        b0_2_1_2_3,
                        b0_2_1_2_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rs1",
                    bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Rs2 - Rs15

        // Next day (2024-08-12), Rt1 should be added to the same bucket as Rs1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rt1", dates[20][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                b0_2_1_3,
                b0_2_1_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rt1",
                    bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Rt2 - Rt15

        // Next day (2024-08-13), Ru1 should be added to the same bucket as Rt1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("ru1", dates[21][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                b0_2_1_3,
                b0_2_1_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ru1",
                    bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Ru2 - Ru15

        // Next day (2024-08-14), Rv1 should be added to the same bucket as Ru1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rv1", dates[22][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                b0_2_1_3,
                b0_2_1_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rv1",
                    bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Rv2 - Rv15

        // Next day (2024-08-15), Rw1 should be added to the same bucket as Rv1.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rw1", dates[23][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                b0_2_1_3,
                b0_2_1_4,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rw1",
                    bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Rw2 - Rw15

        // Next day (2024-08-16), Rx1 - Rx5 should be added to the same bucket as Rw1.
        for (let i = 1; i < 6; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rx${i}`, dates[24][i - 1]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                    b0_2_1_3,
                    b0_2_1_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                            b0_2_1_3,
                            b0_2_1_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rx${i}`,
                        bucket: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                            b0_2_1_3,
                            b0_2_1_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Rx6 should split the bucket recursively until it has to make a new page.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("rx6", dates[24][5]));
        await outputPromise;

        // Expected splits
        const [b0_2_1_3_0, b0_2_1_3_1, b0_2_1_3_2, b0_2_1_3_3, b0_2_1_3_4] =
            splitTimespan(b0_2_1_3, b0_2_1_4, 4);

        testOutput(
            output,
            `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                b0_2_1_3_3,
                b0_2_1_3_4,
            )}_0/`,
            [
                `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                    b0_2_1_3,
                    b0_2_1_4,
                )}_0/`,
            ],
            [
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: true,
                    relations: 8,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_0.toISOString())}_${ts(
                        b0_2_1_3_0,
                        b0_2_1_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_1.toISOString())}_${ts(
                        b0_2_1_3_1,
                        b0_2_1_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_2.toISOString())}_${ts(
                        b0_2_1_3_2,
                        b0_2_1_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
            ],
            [
                {
                    id: "rs1",
                    bucket: `${encodeURIComponent(b0_2_1_3_0.toISOString())}_${ts(
                        b0_2_1_3_0,
                        b0_2_1_3_1,
                    )}_0/`,
                },
                {
                    id: "rt1",
                    bucket: `${encodeURIComponent(b0_2_1_3_0.toISOString())}_${ts(
                        b0_2_1_3_0,
                        b0_2_1_3_1,
                    )}_0/`,
                },
                {
                    id: "ru1",
                    bucket: `${encodeURIComponent(b0_2_1_3_1.toISOString())}_${ts(
                        b0_2_1_3_1,
                        b0_2_1_3_2,
                    )}_0/`,
                },
                {
                    id: "rv1",
                    bucket: `${encodeURIComponent(b0_2_1_3_2.toISOString())}_${ts(
                        b0_2_1_3_2,
                        b0_2_1_3_3,
                    )}_0/`,
                },
                {
                    id: "rw1",
                    bucket: `${encodeURIComponent(b0_2_1_3_2.toISOString())}_${ts(
                        b0_2_1_3_2,
                        b0_2_1_3_3,
                    )}_0/`,
                },
                {
                    id: "rx1",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
                {
                    id: "rx2",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
                {
                    id: "rx3",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
                {
                    id: "rx4",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
                {
                    id: "rx5",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
                {
                    id: "rx6",
                    bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                },
            ],
        );

        // Rx7 - Rx10 should be added to the same bucket as Rx6.
        for (let i = 6; i < 10; i++) {
            output = [];
            const outputPromise = outputListener(outgoingReader, output);
            await incomingWriter.string(
                createInput(`rx${i + 1}`, dates[24][i]),
            );
            await outputPromise;
            testOutput(
                output,
                `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                    b0_2_1_3_3,
                    b0_2_1_3_4,
                )}_0/`,
                [],
                [
                    {
                        id: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                            b0_2_1_3_3,
                            b0_2_1_3_4,
                        )}_0/`,
                        immutable: false,
                        relations: 0,
                    },
                ],
                [
                    {
                        id: `rx${i + 1}`,
                        bucket: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                            b0_2_1_3_3,
                            b0_2_1_3_4,
                        )}_0/`,
                    },
                ],
            );
        }

        // Skip adding Rx11 - Rx15

        // Next day (2024-08-17), Ry1 should be added, making previous buckets immutable.
        output = [];
        outputPromise = outputListener(outgoingReader, output);
        await incomingWriter.string(createInput("ry1", dates[25][0]));
        await outputPromise;
        testOutput(
            output,
            `${encodeURIComponent(b0_2_2.toISOString())}_${ts(
                b0_2_2,
                b0_2_3,
            )}_0/`,
            [],
            [
                {
                    id: `${encodeURIComponent(b0_2_0.toISOString())}_${ts(
                        b0_2_0,
                        b0_2_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1.toISOString())}_${ts(
                        b0_2_1,
                        b0_2_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_2.toISOString())}_${ts(
                        b0_2_2,
                        b0_2_3,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_3.toISOString())}_${ts(
                        b0_2_3,
                        b0_2_4,
                    )}_0/`,
                    immutable: false,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_0.toISOString())}_${ts(
                        b0_2_1_0,
                        b0_2_1_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_1.toISOString())}_${ts(
                        b0_2_1_1,
                        b0_2_1_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_2.toISOString())}_${ts(
                        b0_2_1_2,
                        b0_2_1_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3.toISOString())}_${ts(
                        b0_2_1_3,
                        b0_2_1_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_0.toISOString())}_${ts(
                        b0_2_1_3_0,
                        b0_2_1_3_1,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_1.toISOString())}_${ts(
                        b0_2_1_3_1,
                        b0_2_1_3_2,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_2.toISOString())}_${ts(
                        b0_2_1_3_2,
                        b0_2_1_3_3,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
                {
                    id: `${encodeURIComponent(b0_2_1_3_3.toISOString())}_${ts(
                        b0_2_1_3_3,
                        b0_2_1_3_4,
                    )}_0/`,
                    immutable: true,
                    relations: 0,
                },
            ],
            [
                {
                    id: "ry1",
                    bucket: `${encodeURIComponent(b0_2_2.toISOString())}_${ts(
                        b0_2_2,
                        b0_2_3,
                    )}_0/`,
                },
            ],
        );

        // Skip adding Ry2 - Ry15
    });

    function createInput(id: string, timestamp: Date) {
        return `
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix as: <https://www.w3.org/ns/activitystreams#> .
        @prefix sds: <https://w3id.org/sds#> .
        
        ex:${id} a as:Create ;
            as:published "${timestamp.toISOString()}"^^xsd:dateTime ;
            as:object ex:object1 .
        sds:DataDescription {
            [ ] sds:stream sds:Stream ;
                sds:payload ex:${id} ;
                sds:transactionId "${id}"^^xsd:string ;
                sds:isLastOfTransaction "true"^^xsd:boolean .
        }
        `;
    }

    function ts(date1: Date, date2: Date) {
        return date2.getTime() - date1.getTime();
    }

    function parseBucketizeOutput(output: string) {
        const quads = new Parser({ baseIRI: "" }).parse(output);

        const bucket = quads.find(
            (q) => q.predicate.value === SDS.terms.bucket.value,
        )?.object.value;

        const emptyBuckets = quads
            .filter(
                (q) =>
                    q.predicate.value === SDS.terms.custom("empty").value &&
                    q.object.value === "true",
            )
            .map((q) => q.subject.value);

        const relatedBucketsIds = quads
            .filter(
                (q) =>
                    q.predicate.value === RDF.type &&
                    q.object.value === SDS.terms.custom("Bucket").value,
            )
            .map((q) => q.subject.value);

        const relatedBuckets: {
            id: string;
            immutable: boolean;
            empty: boolean;
            relations: number;
        }[] = [];
        relatedBucketsIds.forEach((bucket) => {
            const immutable =
                quads.find(
                    (q) =>
                        q.subject.value === bucket &&
                        q.predicate.value ===
                            SDS.terms.custom("immutable").value,
                )?.object.value === "true";
            const empty =
                quads.find(
                    (q) =>
                        q.subject.value === bucket &&
                        q.predicate.value === SDS.terms.custom("empty").value,
                )?.object.value === "true";
            const relations = quads.filter(
                (q) =>
                    q.subject.value === bucket &&
                    q.predicate.value === SDS.terms.relation.value,
            ).length;
            relatedBuckets.push({ id: bucket, immutable, relations, empty });
        });

        const membersIds = quads
            .filter((q) => q.predicate.value === SDS.terms.payload.value)
            .map((q) => q.subject.value);
        const members: { id: string; bucket: string }[] = [];
        membersIds.forEach((member) => {
            const id = quads.find(
                (q) =>
                    q.subject.value === member &&
                    q.predicate.value === SDS.terms.payload.value,
            )?.object.value;
            const bucket = quads.find(
                (q) =>
                    q.subject.value === member &&
                    q.predicate.value === SDS.terms.bucket.value,
            )?.object.value;
            if (id && bucket) {
                members.push({
                    id: id,
                    bucket: bucket,
                });
            }
        });

        return { bucket, emptyBuckets, relatedBuckets, members };
    }

    function testOutput(
        output: string[],
        expectedBucket: string,
        expectedEmptyBuckets: string[],
        expectedRelatedBuckets: {
            id: string;
            immutable: boolean;
            relations: number;
        }[],
        expectedMembers: { id: string; bucket: string }[] = [],
    ) {
        // First, check that there is only one output
        expect(output.length).toBe(1);

        // Parse the output
        const { bucket, emptyBuckets, relatedBuckets, members } =
            parseBucketizeOutput(output[0]);

        // Check the bucket
        expect(bucket).toBe(expectedBucket);

        expect(emptyBuckets.length).toBe(expectedEmptyBuckets.length);
        expect(emptyBuckets).toEqual(expectedEmptyBuckets);

        // Check the related buckets
        expect(relatedBuckets.length).toBe(expectedRelatedBuckets.length);
        relatedBuckets.forEach((relatedBucket) => {
            const expected = expectedRelatedBuckets.find(
                (b) => b.id === relatedBucket.id,
            );
            expect(expected).toBeDefined();
            expect(relatedBucket.immutable).toBe(expected!.immutable);
            expect(relatedBucket.relations).toBe(expected!.relations);
        });

        // Check the members
        expect(members.length).toBe(expectedMembers.length);
        members.forEach((member) => {
            const expected = expectedMembers.find(
                (m) => `http://example.org/${m.id}` === member.id,
            );
            expect(expected).toBeDefined();
            expect(member.bucket).toBe(expected!.bucket);
        });

        // Empty the output for the next test
        output.length = 0;
    }

    function splitTimespan(start: Date, end: Date, split: number): Date[] {
        const timespans: Date[] = [];
        const step = Math.round((end.getTime() - start.getTime()) / split);
        for (let i = 0; i < split + 1; i++) {
            const next = new Date(start.getTime() + i * step);
            timespans.push(next);
        }
        return timespans;
    }

    function outputListener(
        outgoing: ReaderInstance,
        output: string[],
    ): Promise<void> {
        const prom = one(outgoing.strings());
        return prom.then((st) => {
            if (st) {
                output.push(st);
            }
        });
    }

    async function setupBucketizer(
        incoming: ReaderInstance,
        outgoing: WriterInstance,
        maxSize: number,
        k: number,
        minBucketSpan: number,
    ): Promise<void> {
        const metaIncoming = createReader();
        const [metaOutgoing] = createWriter();
        const channels = {
            dataInput: incoming,
            metadataInput: metaIncoming,
            dataOutput: outgoing,
            metadataOutput: metaOutgoing,
        };
        const quads = new Parser({ baseIRI: "" }).parse(`
        @prefix tree: <https://w3id.org/tree#> .
        @prefix rdfc: <https://w3id.org/conn/js#> .
        [] rdfc:bucketizeStrategy ([
           a tree:TimebasedFragmentation;
           tree:timestampPath <https://www.w3.org/ns/activitystreams#published>;
           tree:maxSize ${maxSize};
           tree:k ${k};
           tree:minBucketSpan ${minBucketSpan};
        ]) .
        `);
        // Get subject of quad with triple rdf:first
        // eslint-disable-next-line @typescript-eslint/no-non-null-asserted-optional-chain
        const id = quads.find(
            (q) =>
                q.predicate.value ===
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
        )?.subject!;
        const config: BucketizerConfig[] = [
            {
                type: namedNode("https://w3id.org/tree#TimebasedFragmentation"),
                config: {
                    path: pred(
                        namedNode(
                            "https://www.w3.org/ns/activitystreams#published",
                        ),
                    ),
                    pathQuads: {
                        id: namedNode(
                            "https://www.w3.org/ns/activitystreams#published",
                        ),
                        quads: [],
                    },
                    maxSize: maxSize,
                    k: k,
                    minBucketSpan: minBucketSpan,
                },
                quads: {
                    id,
                    quads,
                },
            },
        ];

        const proc = <FullProc<Bucketizer>>new Bucketizer(
            {
                channels,
                savePath: undefined,
                sourceStream: undefined,
                config,
                prefix: "",
                resultingStream: namedNode("https://w3id.org/sds#Stream"),
            },
            logger,
        );
        await proc.init();

        proc.transform();
        await Promise.resolve();
    }
});
