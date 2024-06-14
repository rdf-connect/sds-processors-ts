import type { Stream, Writer } from "@rdfc/js-runner";
import { BlankNode, DataFactory, DefaultGraph, NamedNode, Store } from "n3";
import { PROV, RDF, SDS, SHACL } from "@treecg/types";
import { Quad, Quad_Object, Quad_Subject, Term } from "rdf-js";

export const { namedNode, blankNode, literal, quad } = DataFactory;

export type NBNode = NamedNode | BlankNode;

export type ShapeTransform = (
  id: Term | undefined,
  store: Store,
) => Term | undefined;
export type AddProcess = (used: Term | undefined, store: Store) => Term;
export type DatasetTransform = (used: Term | undefined, store: Store) => Term;

export type QuadsTransform = (quads: Quad[]) => Quad[];

export function getLatestStream(store: Store): NBNode | undefined {
    const streams = store
        .getSubjects(RDF.terms.type, SDS.terms.Stream, null)
        .filter(
            (sub) => store.getQuads(null, PROV.terms.used, sub, null).length === 0,
        );

    if (streams.length != 1) {
        console.error(
            `Couldn't determine previous stream, extected one got ${streams.length}`,
        );
        return undefined;
    }

    return <NBNode>streams[0];
}

export function getLatestShape(streamId: Term, store: Store): NBNode | undefined {
    const shapes = store.getObjects(
        streamId,
        SDS.terms.carries,
        new DefaultGraph(),
    );

    if (shapes.length !== 1) {
        console.error(
            `A sds:stream should carry one type of members, not ${shapes.length}`,
        );
        if (shapes.length == 0) return;
    }

    const shapeIds = shapes.flatMap((id) =>
        store.getObjects(id, SDS.terms.shape, null),
    );

    if (shapeIds.length !== 1) {
        console.error(
            `A sds:stream can only carry one specified shape, not ${shapeIds.length}`,
        );
        return;
    }

    return <NBNode>shapeIds[0];
}

function getLatestDataset(streamId: Term, store: Store): Term | undefined {
    const datasets = store.getObjects(streamId, SDS.terms.dataset, null);

    if (datasets.length !== 1) {
        console.error(
            `A sds:stream should be derived from one dataset, not ${datasets.length}`,
        );
        if (datasets.length == 0) return;
    }

    return datasets[0];
}

export function transformMetadata(
    streamId: Term,
    sourceStream: Term | undefined,
    itemType: string,
    gp: AddProcess,
    shT?: ShapeTransform,
    datasetT?: DatasetTransform,
): QuadsTransform {
    return (quads: Quad[]) => {
        const store = new Store();
        store.addQuads(quads);

        const latest = sourceStream || getLatestStream(store);
        const latestShape = latest ? getLatestShape(latest, store) : undefined;

        const activityId = gp(latest, store);

        const newShape = (shT && shT(latestShape, store)) || undefined;

        let datasetId = latest ? getLatestDataset(latest, store) : undefined;
        if (datasetId && datasetT) {
            datasetId = datasetT(datasetId, store);
        }

        const blank = store.createBlankNode();

        store.addQuad(<Quad_Subject>streamId, RDF.terms.type, SDS.terms.Stream);
        if (datasetId) {
            store.addQuad(
        <Quad_Subject>streamId,
        SDS.terms.dataset,
        <Quad_Object>datasetId,
            );
        }
        store.addQuad(<Quad_Subject>streamId, SDS.terms.carries, blank);
        store.addQuad(
      <Quad_Subject>streamId,
      PROV.terms.wasGeneratedBy,
      <Quad_Object>activityId,
        );

        store.addQuad(blank, RDF.terms.type, namedNode(itemType));

        if (newShape) {
            store.addQuad(blank, SDS.terms.shape, <Quad_Object>newShape);
        }

        const out: Quad[] = [];
        for (const q of store) out.push(<any>q);

        return out;
    };
}

export function createProperty(
    store: Store,
    path: NBNode,
    dataType?: NBNode,
    nodeKind?: NBNode,
    minCount?: number,
    maxCount?: number,
): BlankNode | NamedNode {
    const newId = store.createBlankNode();

    store.addQuad(newId, SHACL.terms.path, path);
    if (dataType) {
        store.addQuad(newId, SHACL.terms.datatype, dataType);
    }

    if (nodeKind) {
        store.addQuad(newId, SHACL.terms.nodeKind, nodeKind);
    }

    if (minCount !== undefined) {
        store.addQuad(newId, SHACL.terms.minCount, literal(minCount));
    }
    if (maxCount !== undefined) {
        store.addQuad(newId, SHACL.terms.maxCount, literal(maxCount));
    }

    return newId;
}

export type SR<T> = {
  [P in keyof T]: Stream<T[P]>;
};

export type SW<T> = {
  [P in keyof T]: Writer<T[P]>;
};
