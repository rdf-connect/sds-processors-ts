import type { Reader, Writer } from "@rdfc/js-runner";
import type {
    BlankNode,
    NamedNode,
    Quad,
    Quad_Object,
    Quad_Subject,
    Term,
} from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { PROV, RDF, SDS, SHACL } from "@treecg/types";
import { getObjects, getSubjects } from "./utils";

const df = new DataFactory();

export type NBNode = NamedNode | BlankNode;

export type ShapeTransform = (
    id: Term | undefined,
    store: RdfStore,
) => Term | undefined;
export type AddProcess = (used: Term | undefined, store: RdfStore) => Term;
export type DatasetTransform = (
    used: Term | undefined,
    store: RdfStore,
) => Term;

export type QuadsTransform = (quads: Quad[]) => Promise<Quad[]>;

export async function getLatestStream(
    store: RdfStore,
): Promise<NBNode | undefined> {
    const streams = (
        await getSubjects(store, RDF.terms.type, SDS.terms.Stream)
    ).filter(
        (sub) => store.getQuads(null, PROV.terms.used, sub, null).length === 0,
    );

    if (streams.length != 1) {
        console.error(
            `Couldn't determine previous stream, expected one got ${streams.length}`,
        );
        return undefined;
    }

    return <NBNode>streams[0];
}

export async function getLatestShape(
    streamId: Term,
    store: RdfStore,
): Promise<NBNode | undefined> {
    const shapes = await getObjects(
        store,
        streamId,
        SDS.terms.carries,
        df.defaultGraph(),
    );

    if (shapes.length !== 1) {
        console.error(
            `A sds:stream should carry one type of members, not ${shapes.length}`,
        );
        if (shapes.length == 0) return;
    }

    const shapeIds = (
        await Promise.all(
            shapes.map(async (id) => {
                return await getObjects(store, id, SDS.terms.shape);
            }),
        )
    ).flat();

    if (shapeIds.length !== 1) {
        console.error(
            `A sds:stream can only carry one specified shape, not ${shapeIds.length}`,
        );
        return;
    }

    return <NBNode>shapeIds[0];
}

async function getLatestDataset(
    streamId: Term,
    store: RdfStore,
): Promise<Term | undefined> {
    const datasets = await getObjects(store, streamId, SDS.terms.dataset);

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
    return async (quads: Quad[]) => {
        const store = RdfStore.createDefault();
        quads.forEach((q) => store.addQuad(q));

        const latest = sourceStream || (await getLatestStream(store));
        const latestShape = latest
            ? await getLatestShape(latest, store)
            : undefined;

        const activityId = gp(latest, store);

        const newShape = (shT && shT(latestShape, store)) || undefined;

        let datasetId = latest
            ? await getLatestDataset(latest, store)
            : undefined;
        if (datasetId && datasetT) {
            datasetId = datasetT(datasetId, store);
        }

        const blank = df.blankNode();

        store.addQuad(
            df.quad(<Quad_Subject>streamId, RDF.terms.type, SDS.terms.Stream),
        );
        if (datasetId) {
            store.addQuad(
                df.quad(
                    <Quad_Subject>streamId,
                    SDS.terms.dataset,
                    <Quad_Object>datasetId,
                ),
            );
        }
        store.addQuad(
            df.quad(<Quad_Subject>streamId, SDS.terms.carries, blank),
        );
        store.addQuad(
            df.quad(
                <Quad_Subject>streamId,
                PROV.terms.wasGeneratedBy,
                <Quad_Object>activityId,
            ),
        );

        // If itemType does not exist yet, add it.
        if (
            store.getQuads(null, RDF.terms.type, df.namedNode(itemType))
                .length === 0
        ) {
            store.addQuad(
                df.quad(blank, RDF.terms.type, df.namedNode(itemType)),
            );
        }

        if (newShape) {
            store.addQuad(
                df.quad(blank, SDS.terms.shape, <Quad_Object>newShape),
            );
        }

        return store.getQuads();
    };
}

export function createProperty(
    store: RdfStore,
    path: NBNode,
    dataType?: NBNode,
    nodeKind?: NBNode,
    minCount?: number,
    maxCount?: number,
): BlankNode | NamedNode {
    const newId = df.blankNode();

    store.addQuad(df.quad(newId, SHACL.terms.path, path));
    if (dataType) {
        store.addQuad(df.quad(newId, SHACL.terms.datatype, dataType));
    }

    if (nodeKind) {
        store.addQuad(df.quad(newId, SHACL.terms.nodeKind, nodeKind));
    }

    if (minCount !== undefined) {
        store.addQuad(
            df.quad(
                newId,
                SHACL.terms.minCount,
                df.literal(minCount.toString()),
            ),
        );
    }
    if (maxCount !== undefined) {
        store.addQuad(
            df.quad(
                newId,
                SHACL.terms.maxCount,
                df.literal(maxCount.toString()),
            ),
        );
    }

    return newId;
}
