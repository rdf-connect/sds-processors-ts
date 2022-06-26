import { BlankNode, NamedNode, Quad, Store, Term } from "n3";
import { createProperty, literal, NBNode, SR, SW, transformMetadata } from "./core";
import { EX, PPLAN, PROV, RDF, SHACL, XSD } from "@treecg/types";


function shapeTransform(id: Term | undefined, store: Store): BlankNode | NamedNode {
    const newId = store.createBlankNode();
    if (id) {
        const quads = store.getQuads(id, null, null, null);
        store.addQuads(quads);
        return newId
    }

    const intTerm = XSD.terms.integer;

    const p1 = createProperty(store, <NBNode>EX.terms.custom("x"), <NBNode>intTerm, undefined, 1, 1);
    const p2 = createProperty(store, <NBNode>EX.terms.custom("y"), <NBNode>intTerm, undefined, 1, 1);

    store.addQuad(newId, RDF.terms.type, SHACL.terms.NodeShape)
    store.addQuad(newId, SHACL.terms.targetClass, EX.terms.custom("Point"));

    store.addQuad(newId, SHACL.terms.property, p1);
    store.addQuad(newId, SHACL.terms.property, p2);

    return newId;
}

function addProcess(id: NBNode | undefined, store: Store): NBNode {
    const newId = store.createBlankNode();
    const time = new Date().getTime();

    store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
    if (id)
        store.addQuad(newId, PROV.terms.used, id);
    store.addQuad(newId, PROV.terms.startedAtTime, literal(time));

    return newId;
}

type Data = { "metadata": Quad[] };
export function updateMetadata(sr: SR<Data>, sw: SW<Data>) {
    const f = transformMetadata(shapeTransform, addProcess, "sds:Member");

    sr.metadata.data(
        quads => sw.metadata.push(f(quads))
    );
}
