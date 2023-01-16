import { createBucketizerLD } from "@treecg/bucketizers";
import { writeFileSync } from "fs";
import { readFile } from "fs/promises";
import { Quad, Parser, Store, DataFactory } from "n3";
import * as N3 from "n3";
import { literal, NBNode, SR, SW, transformMetadata } from "./core";
import { Cleanup } from './exitHandler';
import { LDES, PPLAN, PROV, RDF, SDS } from "@treecg/types";
import { Stream, Writer } from "@treecg/connector-types";

type Data = { "data": Quad[], "metadata": Quad[] };
const { namedNode, quad } = DataFactory;


async function readState(path: string): Promise<any | undefined> {
    try {
        const str = await readFile(path, { "encoding": "utf-8" });
        return JSON.parse(str);
    } catch (e) {
        return
    }
}

async function writeState(path: string, content: any): Promise<void> {
    if (path) {
        const str = JSON.stringify(content);
        writeFileSync(path, str, { encoding: "utf-8" })
    }
}

function addProcess(id: NBNode | undefined, store: Store, strategyId: NBNode, bucketizeConfig: Quad[]): NBNode {
    const newId = store.createBlankNode();
    const time = new Date().getTime();

    store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
    store.addQuad(newId, RDF.terms.type, LDES.terms.Bucketization);

    store.addQuads(bucketizeConfig);

    store.addQuad(newId, PROV.terms.startedAtTime, literal(time));
    store.addQuad(newId, PROV.terms.used, strategyId);
    if (id)
        store.addQuad(newId, PROV.terms.used, id);

    return newId;
}
function parseQuads(quads: string): Quad[] {
    console.log("Parsing quads!");
    const parser = new N3.Parser();
    return parser.parse(quads);
}

export async function doTheBucketization(
    dataReader: Stream<Quad[]>,
    metadataReader: Stream<string>,
    dataWriter: Writer<Quad[]>,
    metadataWriter: Writer<Quad[]>,
    location: string,
    savePath: string,
    sourceStream: string | undefined,
    resultingStream: string
) {
    const sr = { metadata: metadataReader, data: dataReader };
    const sw = { metadata: metadataWriter, data: dataWriter };

    console.log("Reader", sr); console.log("Writer", sw);
    console.log("location", location, "savePath", savePath, "sourceStream", sourceStream, "resultingStream", resultingStream);

    const content = await readFile(location, { encoding: "utf-8" });
    const quads = new Parser().parse(content);

    const quadMemberId = <NBNode>quads.find(quad =>
        quad.predicate.equals(RDF.terms.type) && quad.object.equals(LDES.terms.BucketizeStrategy)
    )!.subject;

    const f = transformMetadata(
        namedNode(resultingStream),
        sourceStream ? namedNode(sourceStream) : undefined,
        "sds:Member",
        (x, y) => addProcess(x, y, quadMemberId, quads)
    );
    sr.metadata.data(
        quads => sw.metadata.push(f(parseQuads(quads)))
    );

    if (sr.metadata.lastElement) {
        sw.metadata.push(f(parseQuads(sr.metadata.lastElement)));
    }

    const state = await readState(savePath);
    const bucketizer = await createBucketizerLD(quads);

    if (state)
        bucketizer.importState(state);

    // Cleanup(async () => {
    //     const state = bucketizer.exportState()
    //     await writeState(savePath, state);
    // })

    sr.data.data(async (t: Quad[]) => {
        if (!t.length) return;

        const members = [...new Set(t.filter(q => q.predicate.equals(SDS.terms.custom("payload"))).map(q => q.object))];
        if (members.length > 1) {
            console.error("Detected more members ids than expected");
        }

        if (members.length === 0) return;

        const sub = members[0].value;
        const extras = <Quad[]><unknown>bucketizer.bucketize(t, sub);


        const recordId = extras.find(q => q.predicate.equals(SDS.terms.payload))!.subject;

        t.push(...<Quad[]>extras);
        t.push(quad(recordId, SDS.terms.stream, namedNode(resultingStream)));
        t.push(quad(recordId, RDF.terms.type, SDS.terms.Member));

        console.log("Pushing thing bucketized!")
        await sw.data.push(t);
    });
}

