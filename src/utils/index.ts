import { NamedNode, Quad, Term } from "@rdfjs/types";
import { Parser, StreamParser } from "n3";
import { SDS, createUriAndTermNamespace } from "@treecg/types";
import { BasicLensM, Cont } from "rdf-lens";
import { RdfStore } from "rdf-stores";

import { Logger } from "winston";
import { Readable } from "stream";

export const GEO = createUriAndTermNamespace(
    "http://www.opengis.net/ont/geosparql#",
    "wktLiteral",
);

export const SDS_GRAPH: NamedNode = SDS.terms.custom("DataDescription");

export type RdfThing = {
    id: Term;
    quads: Quad[];
};

export type BucketRelation = {
    type: Term;
    target: Term;
    value?: Term;
    path?: RdfThing;
};

export type Member = {
    id: string;
    timestamp: number;
};

export class Bucket {
    id: Term;
    parent?: Bucket;
    root?: boolean;
    immutable?: boolean;
    links: BucketRelation[];
    empty: boolean = false;
    addMember: (memberId: string) => void;

    constructor(
        id: Term,
        links: BucketRelation[],
        root?: boolean,
        immutable?: boolean,
        parent?: Bucket,
    ) {
        this.id = id;
        this.root = root;
        this.immutable = immutable;
        this.links = links;
        this.parent = parent;
    }
}

export class Record {
    stream: Term;
    data: RdfThing;
    bucket?: Bucket;

    constructor(data: RdfThing, stream: Term, bucket?: Bucket) {
        this.stream = stream;
        this.data = data;
        this.bucket = bucket;
    }
}

export async function getSubjects(
    store: RdfStore,
    pred?: Term,
    object?: Term,
    graph?: Term,
): Promise<Term[]> {
    const quads = await store.match(null, pred, object, graph).toArray();
    return quads.map((x) => x.subject);
}

export async function getObjects(
    store: RdfStore,
    subject?: Term,
    pred?: Term,
    graph?: Term,
): Promise<Term[]> {
    const quads = await store.match(subject, pred, null, graph).toArray();
    return quads.map((x) => x.object);
}

export function getOrDefaultMap<T1, T2>(
    map: Map<T1, T2>,
    key: T1,
    def: T2,
): T2 {
    return map.get(key) || map.set(key, def).get(key)!;
}

export function maybeParse(data: Quad[] | string): Quad[] {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        return parse.parse(<string>data);
    } else {
        return data;
    }
}

export async function loadIntoStore(
    data: string | Readable,
    store: RdfStore,
): Promise<void> {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        parse.parse(<string>data).forEach((quad) => store.addQuad(quad));
    } else if (data instanceof Readable) {
        const parse = new StreamParser();
        await new Promise((resolve, reject) => {
            store
                .import(data.pipe(parse))
                .on("end", resolve)
                .on("error", reject);
        });
    }
}

export function findAndValidateRecordTimestamp(
    record: Record,
    path: BasicLensM<Cont, { value: string; literal?: Term }>,
    lastMemberTimestamp: number,
    logger: Logger,
): Date | undefined | null {
    let recordTimestamp: Date | undefined = undefined;
    if (path) {
        const values = path
            .execute(record.data)
            .filter(
                (x, i, arr) => arr.findIndex((y) => x.value === y.value) == i,
            )
            .filter((value) => value.literal);

        if (values.length !== 1) {
            logger.error(
                `Expected exactly one timestamp value, got ${values.length}. Ignoring record '${record.data.id.value}'.`,
            );
            return null;
        }

        recordTimestamp = new Date(values[0].literal!.value);

        // Check if the record is out of order.
        if (recordTimestamp.getTime() < lastMemberTimestamp) {
            logger.error(
                `Record timestamp is before the last record timestamp. Are your records out of order? Ignoring record '${record.data.id.value}'.`,
            );
            return null;
        }
    }
    return recordTimestamp;
}
