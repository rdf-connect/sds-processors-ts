import { describe, test, expect } from "@jest/globals";
import { SimpleStream } from "@ajuvercr/js-runner";
import { DataFactory, Parser, Store } from "n3";
import { sdsify } from "../src/sdsify";
import { RDF, SDS } from "@treecg/types";

const { namedNode, literal } = DataFactory;

describe("Functional tests for the sdsify function", () => {

    const STREAM_ID = namedNode("http://ex.org/myStream");
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

        const store = new Store();

        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => {
            // Check there number of members
            expect(store.getObjects(null, SDS.payload, null).length).toBe(2);

            // Check all properties are extracted for members
            expect(store.getQuads(null, "http://ex.org/prop1", literal("some value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop2", literal("some other value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/nestedProp1", literal(50), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop4", literal("some value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop5", literal(15), null).length).toBe(1);
        });

        // Execute function
        sdsify(input, output, STREAM_ID);

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    })

    test("Extraction of particular entity type based on a SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = new Store();

        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => {
            // Check there number of members
            expect(store.getObjects(null, SDS.payload, null).length).toBe(1);

            // Check all properties are extracted for members
            expect(store.getQuads(null, "http://ex.org/prop4", literal("some value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop5", literal(15), null).length).toBe(1);
        });

        // Execute function
        sdsify(input, output, STREAM_ID, [SHAPE_1]);

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Partial extraction of particular entity type based on a SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = new Store();

        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => {
            // Check there number of members
            expect(store.getObjects(null, SDS.payload, null).length).toBe(1);

            // Check all properties are extracted for members
            expect(store.getQuads(null, "http://ex.org/prop2", literal("some other value"), null).length).toBe(1);
        });

        // Execute function
        sdsify(input, output, STREAM_ID, [SHAPE_2]);

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Partial extraction of particular entity type with a nested SHACL shape", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = new Store();

        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => {
            // Check there number of members
            expect(store.getObjects(null, SDS.payload, null).length).toBe(1);

            // Check all properties are extracted for members
            expect(store.getQuads(null, RDF.type, namedNode("http://ex.org/SomeClass"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop2", namedNode("B"), null).length).toBe(1);
            expect(store.getQuads(null, RDF.type, namedNode("http://ex.org/SomeOtherClass"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop3", literal("another value"), null).length).toBe(1);
        });

        // Execute function
        sdsify(input, output, STREAM_ID, [SHAPE_3]);

        // Push some data in
        await input.push(INPUT_2);
        await input.end();
    });

    test("(Partial) extraction of multiples entity types based on SHACL shapes", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = new Store();

        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => {
            // Check there number of members
            expect(store.getObjects(null, SDS.payload, null).length).toBe(2);

            // Check all properties are extracted for members
            expect(store.getQuads(null, "http://ex.org/prop2", literal("some other value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop4", literal("some value"), null).length).toBe(1);
            expect(store.getQuads(null, "http://ex.org/prop5", literal(15), null).length).toBe(1);
        });

        // Execute function
        sdsify(input, output, STREAM_ID, [SHAPE_1, SHAPE_2]);

        // Push some data in
        await input.push(INPUT_1);
        await input.end();
    });

    test("Failure on shape with multiple main node shapes", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();

        const store = new Store();
        output.data(data => {
            store.addQuads(new Parser().parse(data));
        }).on("end", () => { });

        // Execute function
        sdsify(input, output, STREAM_ID, [BAD_SHAPE_1]);
        try {
            // Push some data in
            expect(await input.push(INPUT_1)).toThrow(Error);
        } catch (err) {
            expect(err.message).toBe("There are multiple main node shapes in a given shape. Unrelated shapes must be given as separate shape filters");
        }
    });
});