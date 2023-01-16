import { Stream, Writer } from "@treecg/connector-types";
import { BlankNode, DataFactory, DefaultGraph, NamedNode, Quad, Store, Term, Writer as NWriter } from "n3";
import { PROV, RDF, SDS, SHACL } from "@treecg/types";


export const { namedNode, blankNode, literal } = DataFactory;


export type NBNode = NamedNode | BlankNode;

export type ShapeTransform = (id: NBNode | undefined, store: Store) => NBNode | undefined;
export type AddProcess = (used: NBNode | undefined, store: Store) => NBNode;
export type DatasetTransform = (used: NBNode | undefined, store: Store) => NBNode;

export type QuadsTransform = (quads: Quad[]) => Quad[];

function getLatestStream(store: Store): NBNode | undefined {
    const streams = store.getSubjects(RDF.terms.type, SDS.terms.Stream, null)
        .filter(sub => store.getQuads(null, PROV.terms.used, sub, null).length === 0);

    if (streams.length != 1) {
        console.error(`Couldn't determine previous stream, extected one got ${streams.length}`);
        return undefined;
    }

    return <NBNode>streams[0];
}

function getLatestShape(streamId: Term, store: Store): NBNode | undefined {
    console.log("Found predicates for stream", streamId,
        store.getPredicates(streamId, null, null)
    )
    const shapes = store.getObjects(streamId, SDS.terms.carries, new DefaultGraph());

    if (shapes.length !== 1) {
        console.error(`A sds:stream should carry one type of members, not ${shapes.length}`)
        if (shapes.length == 0) return
    }

    const shapeIds = shapes.flatMap(id =>
        store.getObjects(id, SDS.terms.shape, null)
    );

    if (shapeIds.length !== 1) {
        console.error(`A sds:stream can only carry one specified shape, not ${shapeIds.length}`)
        return
    }

    console.log("Found valid stream", shapeIds[0]);
    return <NBNode>shapeIds[0]
}

function getLatestDataset(streamId: Term, store: Store): NBNode | undefined {
    const datasets = store.getObjects(streamId, SDS.terms.dataset, null);

    if (datasets.length !== 1) {
        console.error(`A sds:stream should be derived from one dataset, not ${datasets.length}`)
        if (datasets.length == 0) return
    }

    return <NBNode>datasets[0];
}

export function transformMetadata(streamId: NBNode, sourceStream: NamedNode | undefined, itemType: string, gp: AddProcess, shT?: ShapeTransform, datasetT?: DatasetTransform): QuadsTransform {
    return (quads: Quad[]) => {
        const store = new Store();
        console.log("handling metadata transform");
        store.addQuads(quads);

        const latest = sourceStream || getLatestStream(store);
        const latestShape = !!latest ? getLatestShape(latest, store) : undefined;

        const activityId = gp(latest, store);

        const newShape = shT && shT(latestShape, store) || undefined;

        let datasetId = !!latest ? getLatestDataset(latest, store) : undefined;
        if (datasetId && datasetT) {
            datasetId = datasetT(datasetId, store);
        }


        const blank = store.createBlankNode();

        store.addQuad(streamId, RDF.terms.type, SDS.terms.Stream);
        if (datasetId) {
            store.addQuad(streamId, SDS.terms.dataset, datasetId);
        }
        store.addQuad(streamId, SDS.terms.carries, blank);
        store.addQuad(streamId, PROV.terms.wasGeneratedBy, activityId);

        store.addQuad(blank, RDF.terms.type, namedNode(itemType));

        if (newShape)
            store.addQuad(blank, SDS.terms.shape, newShape);


        const out: Quad[] = [];
        for (let q of store) out.push(<any>q);

        console.log("returning new metadata");
        return out;
    }
}

export function createProperty(store: Store, path: NBNode, dataType?: NBNode, nodeKind?: NBNode, minCount?: number, maxCount?: number): BlankNode | NamedNode {
    const newId = store.createBlankNode();

    store.addQuad(newId, SHACL.terms.path, path)
    if (dataType)
        store.addQuad(newId, SHACL.terms.datatype, dataType)

    if (nodeKind)
        store.addQuad(newId, SHACL.terms.nodeKind, nodeKind)

    if (minCount !== undefined)
        store.addQuad(newId, SHACL.terms.minCount, literal(minCount))
    if (maxCount !== undefined)
        store.addQuad(newId, SHACL.terms.maxCount, literal(maxCount))

    return newId;
}

export type SR<T> = {
    [P in keyof T]: Stream<T[P]>;
}

export type SW<T> = {
    [P in keyof T]: Writer<T[P]>;
}

