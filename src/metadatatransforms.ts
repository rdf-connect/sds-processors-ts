import { BlankNode, DataFactory, NamedNode, Store } from "n3";
import {
  createProperty,
  literal,
  NBNode,
  SR,
  SW,
  transformMetadata,
} from "./core.js";
import { EX, PPLAN, PROV, RDF, SHACL, XSD } from "@treecg/types";
import { Quad, Quad_Object, Term } from "rdf-js";

const { namedNode } = DataFactory;

function shapeTransform(
  id: Term | undefined,
  store: Store,
): BlankNode | NamedNode {
  const newId = store.createBlankNode();
  if (id) {
    const quads = store.getQuads(id, null, null, null);
    store.addQuads(quads);
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

  store.addQuad(newId, RDF.terms.type, SHACL.terms.NodeShape);
  store.addQuad(newId, SHACL.terms.targetClass, EX.terms.custom("Point"));

  store.addQuad(newId, SHACL.terms.property, p1);
  store.addQuad(newId, SHACL.terms.property, p2);

  return newId;
}

function addProcess(id: Term | undefined, store: Store): Term {
  const newId = store.createBlankNode();
  const time = new Date().toISOString();

  store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
  if (id) {
    store.addQuad(newId, PROV.terms.used, <Quad_Object>id);
  }
  store.addQuad(newId, PROV.terms.startedAtTime, literal(time));

  return newId;
}

type Data = { metadata: Quad[] };
export function updateMetadata(
  sr: SR<Data>,
  sw: SW<Data>,
  sourceStream: string | undefined,
  newStream: string,
) {
  const sourceStreamName = sourceStream ? namedNode(sourceStream) : undefined;
  const newStreamName = namedNode(newStream);
  const f = transformMetadata(
    newStreamName,
    sourceStreamName,
    "sds:Member",
    addProcess,
    shapeTransform,
  );

  sr.metadata.data((quads) => sw.metadata.push(f(quads)));
}
