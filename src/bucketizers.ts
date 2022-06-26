import { createBucketizerLD } from "@treecg/bucketizers";
import { writeFileSync } from "fs";
import { readFile } from "fs/promises";
import { BlankNode, NamedNode, Parser, Quad, Store, Term } from "n3";
import { createProperty, literal, NBNode, SR, SW, transformMetadata } from "./core";
import { Cleanup } from './exitHandler';
import { LDES, PPLAN, PROV, RDF, SHACL } from "@treecg/types";

type Data = { "data": Quad[], "metadata": Quad[] };


async function readState(path: string): Promise<any | undefined> {
    try {
        const str = await readFile(path, { "encoding": "utf-8" });
        return JSON.parse(str);
    } catch (e) {
        return
    }
}

async function writeState(path: string, content: any): Promise<void> {
    const str = JSON.stringify(content);
    writeFileSync(path, str, { encoding: "utf-8" })
}

function shapeTransform(id: Term | undefined, store: Store, property: NBNode): BlankNode | NamedNode {
    const newId = store.createBlankNode();
    if (id) {
        const p1 = createProperty(store, property, undefined, undefined, 1, 1);
        const quads = store.getQuads(id, null, null, null);

        for (let quad of quads) {
            store.addQuad(newId, quad.predicate, quad.object);
        }

        store.addQuad(newId, SHACL.terms.property, p1);
        store.addQuads(quads);
        return newId
    } else {
        throw "no shape transform"

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

export async function doTheBucketization(sr: SR<Data>, sw: SW<Data>, location: string, savePath: string) {
    const content = await readFile(location, { encoding: "utf-8" });
    const quads = new Parser().parse(content);


    const quadMemberId = <NBNode>quads.find(quad =>
        quad.predicate.equals(RDF.terms.type) && quad.object.equals(LDES.terms.BucketizeStrategy)
    )!.subject;

    const bucketProperty = <NBNode>(quads.find(quad =>
        quad.subject.equals(quadMemberId) && quad.predicate.equals(LDES.terms.bucketProperty)
    )?.object || LDES.terms.bucket);


    const f = transformMetadata((x, y) => shapeTransform(x, y, bucketProperty), (x, y) => addProcess(x, y, quadMemberId, quads), "sds:Member");
    sr.metadata.data(
        quads => sw.metadata.push(f(quads))
    );
    if (sr.metadata.lastElement) {
        sw.metadata.push(f(sr.metadata.lastElement));
    }

    const state = await readState(savePath);
    const bucketizer = await createBucketizerLD(quads);
    if (state)
        bucketizer.importState(state);

    Cleanup(async () => {
        const state = bucketizer.exportState()
        await writeState(savePath, state);
    })

    sr.data.data(async (t) => {
        if (!t.length) return;

        const sub = t[0].subject;

        bucketizer.bucketize(t, sub.value);

        console.log("Pushing thing bucketized!")
        await sw.data.push(t);
    });
}
