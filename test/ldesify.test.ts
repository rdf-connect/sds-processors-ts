import { describe, expect, test } from "vitest";
import { DataFactory } from "rdf-data-factory";
import { Parser, Quad_Object, Writer } from "n3";
import { LdesifySDS } from "../lib/ldesify";
import { SDS } from "@treecg/types";
import { Quad, Term } from "@rdfjs/types";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils";
import { readStrings, strs } from "./utils";
import { createLogger, transports } from "winston";

import type { FullProc } from "@rdfc/js-runner";

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
        const runner = createRunner();
        const [inputWriter, inputReader] = channel(runner, "input");
        const [outputWriter, outputReader] = channel(runner, "output");

        const outs: string[] = [];

        readStrings(outputReader, outs);
        const sts = strs(outputReader);
        const logger = createLogger({
            transports: [new transports.Console()],
        });

        const proc = <FullProc<LdesifySDS>>new LdesifySDS(
            {
                reader: inputReader,
                writer: outputWriter,
                targetStream: stream,
                statePath: undefined,
                sourceStream: undefined,
                isVersionOfPathM: undefined,
                modifiedPathM: undefined,
            },
            logger,
        );
        await proc.init();
        proc.transform();

        const p1 = df.namedNode("Person1");
        const p2 = df.namedNode("Person2");
        inputWriter.string(BuildSds(manQuads(p1, 45), p1));
        await new Promise((res) => setTimeout(res, 20));
        expect(outs.length).toBe(1);
        inputWriter.string(BuildSds(manQuads(p1, 45), p1));
        await new Promise((res) => setTimeout(res, 20));
        expect(outs.length).toBe(1);

        inputWriter.string(BuildSds(manQuads(p1, 46), p1));
        await new Promise((res) => setTimeout(res, 20));
        expect(outs.length).toBe(2);
        inputWriter.string(BuildSds(manQuads(p2, 46), p2));
        await new Promise((res) => setTimeout(res, 20));
        expect(outs.length).toBe(3);
        inputWriter.string(BuildSds(manQuads(p1, 46), p1));
        await inputWriter.close();
        const strings = await sts;
        expect(strings.length).toBe(3);
    });
});
