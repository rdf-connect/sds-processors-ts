import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import {
    createProperty,
    NBNode,
    SR,
    SW,
    transformMetadata,
} from "./core.js";
import { EX, PPLAN, PROV, RDF, SHACL, XSD } from "@treecg/types";
import { BlankNode, NamedNode, Quad, Quad_Object, Term } from "@rdfjs/types";

const df = new DataFactory();

function shapeTransform(
    id: Term | undefined,
    store: RdfStore,
): BlankNode | NamedNode {
    const newId = df.blankNode();
    if (id) {
        const quads = store.getQuads(id);
        quads.forEach(q => store.addQuad(q));
        return newId;
    }

    const intTerm = XSD.terms.integer;

    const p1 = createProperty(
        store,
    <NBNode>EX.terms.custom("x"),
    <NBNode>intTerm,
    undefined,
    1,
    1,
    );
    const p2 = createProperty(
        store,
    <NBNode>EX.terms.custom("y"),
    <NBNode>intTerm,
    undefined,
    1,
    1,
    );

    store.addQuad(df.quad(newId, RDF.terms.type, SHACL.terms.NodeShape));
    store.addQuad(df.quad(newId, SHACL.terms.targetClass, EX.terms.custom("Point")));

    store.addQuad(df.quad(newId, SHACL.terms.property, p1));
    store.addQuad(df.quad(newId, SHACL.terms.property, p2));

    return newId;
}

function addProcess(id: Term | undefined, store: RdfStore): Term {
    const newId = df.blankNode();
    const time = new Date().toISOString();

    store.addQuad(df.quad(newId, RDF.terms.type, PPLAN.terms.Activity));
    if (id) {
        store.addQuad(df.quad(newId, PROV.terms.used, <Quad_Object>id));
    }
    store.addQuad(df.quad(newId, PROV.terms.startedAtTime, df.literal(time)));

    return newId;
}

type Data = { metadata: Quad[] };
export function updateMetadata(
    sr: SR<Data>,
    sw: SW<Data>,
    sourceStream: string | undefined,
    newStream: string,
) {
    const sourceStreamName = sourceStream ? df.namedNode(sourceStream) : undefined;
    const newStreamName = df.namedNode(newStream);
    const f = transformMetadata(
        newStreamName,
        sourceStreamName,
        "sds:Member",
        addProcess,
        shapeTransform,
    );

    sr.metadata.data(async (quads) => sw.metadata.push(await f(quads)));
}

