import { describe, expect, test } from "vitest";
import { SimpleStream } from "@rdfc/js-runner";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Parser } from "n3";
import { getObjects } from "../lib/utils";
import { sdsify } from "../lib/sdsify";
import { LDES, RDF, SDS, XSD } from "@treecg/types";

const df = new DataFactory();

describe("Functional tests for the sdsify function", () => {
    const STREAM_ID = df.namedNode("http://ex.org/myStream");
    const INPUT_1 = `
        @prefix ex: <http://ex.org/>.

        <A> a ex:SomeClass;
            ex:prop1 "some value";
            ex:prop2 "some other value";
            ex:prop3 [
                a ex:SomeNestedClass;
                ex:nestedProp1 50
            ].

        <B> a ex:SomeOtherClass;
            ex:prop4 "some value";
            ex:prop5 15.
    `;
    const INPUT_2 = `
        @prefix ex: <http://ex.org/>.

        <A> a ex:SomeClass;
            ex:prop1 "some value";
            ex:prop2 <B>.
        
        <B> a ex:SomeOtherClass;
            ex:prop3 "another value".
    `;
    const INPUT_3 = `
        @prefix ex:  <http://ex.org/>.
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.

        <A> a ex:SomeClass;
            ex:prop1 "some value A";
            ex:timestamp "2024-01-05T09:00:00.000Z"^^xsd:dateTime.

        <B> a ex:SomeClass;
            ex:prop1 "some value B";
            ex:timestamp "2024-01-05T10:00:00.000Z"^^xsd:dateTime.
        
        <C> a ex:SomeClass;
            ex:prop1 "some value C";
            ex:timestamp "2024-01-05T07:00:00.000Z"^^xsd:dateTime.
    `;
    const SHAPE_1 = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeOtherClass;
            sh:property [
                sh:path ex:prop4
            ], [
                sh:path ex:prop5
            ].
    `;
    const SHAPE_2 = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:closed true;
            sh:property [
                sh:path ex:prop2
            ].
    `;
    const SHAPE_3 = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:closed true;
            sh:property [
                sh:path rdf:type
            ], [
                sh:path ex:prop2;
                sh:node [
                    a sh:NodeShape;
                    sh:targetClass ex:SomeOtherClass;
                    sh:property [
                        sh:path ex:prop3
                    ]
                ]
            ].
    `;
    const SHAPE_4 = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass.
    `;
    const MULTI_SHAPE = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:xone (<A> <B>).

        <A> a sh:NodeShape;
            sh:targetClass ex:SomeOtherClass;
            sh:property [
                sh:path ex:prop4
            ], [
                sh:path ex:prop5
            ].

        <B> a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:closed true;
            sh:property [
                sh:path ex:prop2
            ].
    `;
    const BAD_SHAPE_1 = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:closed true;
            sh:property [
                sh:path ex:prop2
            ].

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeOtherClass.
    `;

    test("Default extraction without a given shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();

        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(2);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop1"),
                        df.literal("some value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop2"),
                        df.literal("some other value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/nestedProp1"),
                        df.literal("50", XSD.terms.integer),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop4"),
                        df.literal("some value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop5"),
                        df.literal("15", XSD.terms.integer),
                        null,
                    ).length,
                ).toBe(1);
            });

        // Execute function
        await sdsify(input, output, STREAM_ID);

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Extraction of particular entity type based on a SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();

        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(1);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop4"),
                        df.literal("some value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop5"),
                        df.literal("15", XSD.terms.integer),
                        null,
                    ).length,
                ).toBe(1);
            });

        // Execute function
        await sdsify(
            input,
            output,
            STREAM_ID,
            [df.namedNode("http://ex.org/SomeOtherClass")],
            undefined,
            SHAPE_1,
        );

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Partial extraction of particular entity type based on a SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();

        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(1);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop2"),
                        df.literal("some other value"),
                        null,
                    ).length,
                ).toBe(1);

                // Check some properties were excluded
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop1"),
                        null,
                        null,
                    ).length,
                ).toBe(0);
            });

        // Execute function
        await sdsify(
            input,
            output,
            STREAM_ID,
            [df.namedNode("http://ex.org/SomeClass")],
            undefined,
            SHAPE_2,
        );

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Partial extraction of particular entity type with a nested SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();

        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(1);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        RDF.terms.type,
                        df.namedNode("http://ex.org/SomeClass"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop2"),
                        df.namedNode("B"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        RDF.terms.type,
                        df.namedNode("http://ex.org/SomeOtherClass"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop3"),
                        df.literal("another value"),
                        null,
                    ).length,
                ).toBe(1);
            });

        // Execute function
        await sdsify(
            input,
            output,
            STREAM_ID,
            [df.namedNode("http://ex.org/SomeClass")],
            undefined,
            SHAPE_3,
        );

        // Push some data in
        await input.push(INPUT_2);
        await input.end();
    });

    test("(Partial) extraction of multiples entity types based on SHACL shapes", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();

        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(2);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop2"),
                        df.literal("some other value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop4"),
                        df.literal("some value"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop5"),
                        df.literal("15", XSD.terms.integer),
                        null,
                    ).length,
                ).toBe(1);
            });

        // Execute function
        await sdsify(
            input,
            output,
            STREAM_ID,
            undefined,
            undefined,
            MULTI_SHAPE,
        );

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Failure on shape with multiple main node shapes", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();
        output
            .data((data) => {
                new Parser().parse(data).forEach((q) => store.addQuad(q));
            })
            .on("end", () => {});

        // Execute function
        try {
            await sdsify(
                input,
                output,
                STREAM_ID,
                undefined,
                undefined,
                BAD_SHAPE_1,
            );

            // Push some data in
            expect(true).toBeFalsy();
        } catch (err) {
            console.log("Got error!");
            expect(err.message).toBe(
                "There are multiple main node shapes in a given shape." +
                    " Use sh:xone or sh:or to provide multiple unrelated shapes together.",
            );
        }
    });

    test("Time stamp-based ordering of SHACL-based extraction", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = RdfStore.createDefault();
        const timestamps: string[] = [];

        output
            .data(async (data) => {
                const quads = new Parser().parse(data);
                const subj = quads[0].subject;
                quads.forEach((q) => store.addQuad(q));
                timestamps.push(
                    (
                        await getObjects(
                            store,
                            subj,
                            df.namedNode("http://ex.org/timestamp"),
                        )
                    )[0].value,
                );
            })
            .on("end", async () => {
                // Check there number of members
                expect(
                    (await getObjects(store, undefined, SDS.terms.payload))
                        .length,
                ).toBe(3);

                // Check all properties are extracted for members
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop1"),
                        df.literal("some value A"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop1"),
                        df.literal("some value B"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/prop1"),
                        df.literal("some value C"),
                        null,
                    ).length,
                ).toBe(1);
                expect(
                    store.getQuads(
                        null,
                        df.namedNode("http://ex.org/timestamp"),
                        null,
                        null,
                    ).length,
                ).toBe(3);

                // Check all members belong to the same transaction and last one is marked as such
                const tIds = await getObjects(
                    store,
                    undefined,
                    LDES.terms.custom("transactionId"),
                );
                expect(tIds.every((id) => id.value === tIds[0].value));
                expect(
                    (
                        await getObjects(
                            store,
                            undefined,
                            LDES.terms.custom("isLastOfTransaction"),
                        )
                    ).length,
                ).toBe(1);

                let currT = 0;
                for (const ts of timestamps) {
                    const tsv = new Date(ts).getTime();
                    expect(tsv).toBeGreaterThanOrEqual(currT);
                    currT = tsv;
                }
            });

        // Execute function
        await sdsify(
            input,
            output,
            STREAM_ID,
            undefined,
            df.namedNode("http://ex.org/timestamp"),
            SHAPE_4,
        );

        // Push some data in
        await input.push(INPUT_3);
        await input.end();
    });
});
