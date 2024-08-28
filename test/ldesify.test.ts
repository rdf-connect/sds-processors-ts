import { describe, expect, test } from "vitest";
import { SimpleStream } from "@rdfc/js-runner";
import { DataFactory } from "rdf-data-factory";
import { Parser, Quad_Object, Writer } from "n3";
import { ldesify_sds } from "../lib/ldesify";
import { SDS } from "@treecg/types";
import { Quad, Term } from "@rdfjs/types";

const df = new DataFactory();

const stream = df.namedNode("http://myStream.org/ns#");
function BuildSds(data: Quad[], id: Term): string {
    const sdsId = df.blankNode();
    const quads = data.slice();

    quads.push(
        df.quad(
            sdsId,
            SDS.terms.payload,
            <Quad_Object>id,
            SDS.terms.custom("DataDescription"),
        ),
        df.quad(
            sdsId,
            SDS.terms.stream,
            <Quad_Object>stream,
            SDS.terms.custom("DataDescription"),
        ),
    );

    return new Writer().quadsToString(quads);
}

function manQuads(id: Term, age: number): Quad[] {
    return new Parser().parse(
        `
@prefix foaf: <http://xmlns.com/foaf/0.1/>.
<${id.value}> a foaf:Human;
  foaf:age ${age}.
`,
    );
}

describe("Functional tests for the ldesify function", () => {
    test("LDESify works", async () => {
        const input = new SimpleStream<string>();
        const output = new SimpleStream<string>();
        const outs: string[] = [];
        output.data((x) => {
            outs.push(x);
        });
        ldesify_sds(input, output, undefined, undefined, stream);

        const p1 = df.namedNode("Person1");
        const p2 = df.namedNode("Person2");
        await input.push(BuildSds(manQuads(p1, 45), p1));

        expect(outs.length).toBe(1);
        await input.push(BuildSds(manQuads(p1, 45), p1));
        expect(outs.length).toBe(1);

        await input.push(BuildSds(manQuads(p1, 46), p1));
        expect(outs.length).toBe(2);
        await input.push(BuildSds(manQuads(p2, 46), p2));
        expect(outs.length).toBe(3);
        await input.push(BuildSds(manQuads(p1, 46), p1));
        expect(outs.length).toBe(3);
    });
});
