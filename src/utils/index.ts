import { Quad, Quad_Object, Quad_Subject, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { NBNode } from "../core";
import { Parser, Writer } from "n3";
import { SDS, XSD } from "@treecg/types";
import { BasicLensM, extractShapes, match, Shapes, subject } from "rdf-lens";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";

import { $INLINE_FILE } from "@ajuvercr/ts-transformer-inline-file";

const df = new DataFactory();

export const SDS_GRAPH = SDS.terms.custom("DataDescription");

export const SHAPES_TEXT = $INLINE_FILE("../../configs/sds_shapes.ttl");

export type RdfThing = {
    id: Term;
    quads: Quad[];
};

export type RelationDTO = {
    type: Term;
    target: Term;
    value?: Term | undefined;
    path?: RdfThing;
};

export type BucketDTO = {
    links: RelationDTO[];
    id: Term;
    root?: boolean;
    immutable?: boolean;
    parent?: BucketDTO;
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

function writeRelation(rel: BucketRelation, writer: Writer): Term {
    const id = df.blankNode();
    writer.addQuads([
        df.quad(
            id,
            SDS.terms.relationType,
            <Quad_Object>rel.type,
            SDS.terms.custom("DataDescription"),
        ),
        df.quad(
            id,
            SDS.terms.relationBucket,
            <Quad_Object>rel.target,
            SDS.terms.custom("DataDescription"),
        ),
    ]);

    if (rel.value) {
        writer.addQuad(
            df.quad(
                id,
                SDS.terms.relationValue,
                <Quad_Object>rel.value,
                SDS.terms.custom("DataDescription"),
            ),
        );
    }
    if (rel.path) {
        writer.addQuads([
            df.quad(
                id,
                SDS.terms.relationPath,
                <Quad_Object>rel.path.id,
                SDS.terms.custom("DataDescription"),
            ),
            ...rel.path.quads.map((x) =>
                df.quad(
                    x.subject,
                    x.predicate,
                    x.object,
                    SDS.terms.custom("DataDescription"),
                ),
            ),
        ]);
    }
    return id;
}

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

    static parse(
        bucket: BucketDTO,
        bucket_cache: { [id: string]: Bucket },
    ): Bucket {
        const parent = bucket.parent
            ? Bucket.parse(bucket.parent, bucket_cache)
            : undefined;
        const links = bucket.links.map(({ target, path, value, type }) => ({
            path,
            value,
            type,
            target: target,
        }));
        const out = new Bucket(
            bucket.id,
            links,
            bucket.root,
            bucket.immutable,
            parent,
        );
        bucket_cache[bucket.id.value] = out;
        return out;
    }

    write(writer: Writer) {
        const id = <Quad_Subject>this.id;
        const relations = this.links
            .map((rel) => writeRelation(rel, writer))
            .map((rel) =>
                df.quad(
                    id,
                    SDS.terms.relation,
                    <Quad_Object>rel,
                    SDS.terms.custom("DataDescription"),
                ),
            );

        if (this.root) {
            relations.push(
                df.quad(
                    id,
                    SDS.terms.custom("isRoot"),
                    df.literal("true", XSD.terms.custom("boolean")),
                    SDS.terms.custom("DataDescription"),
                ),
            );
        }

        writer.addQuads(relations);
    }
}

interface RecordDTO {
    stream: Term;
    data: Term;
    /* eslint-disable @typescript-eslint/no-explicit-any */
    bucket: any;
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

    static parse(
        { stream, data, bucket }: RecordDTO,
        store: RdfStore,
        bucket_cache: { [id: string]: Bucket },
    ): Record {
        const quads = store
            .getQuads()
            .filter((q) => q.graph.value !== SDS.custom("DataDescription"));

        let actual_bucket: Bucket | undefined;
        if (bucket) {
            actual_bucket = Bucket.parse(bucket, bucket_cache);
        }

        return new Record({ id: data, quads }, stream, actual_bucket);
    }

    write(writer: Writer) {
        const id = df.blankNode();

        const quads = [
            df.quad(
                id,
                SDS.terms.payload,
                <Quad_Object>this.data.id,
                SDS.terms.custom("DataDescription"),
            ),
            df.quad(
                id,
                SDS.terms.stream,
                <Quad_Object>this.stream,
                SDS.terms.custom("DataDescription"),
            ),
        ];
        if (this.bucket) {
            quads.push(
                df.quad(
                    id,
                    SDS.terms.bucket,
                    <Quad_Object>this.bucket.id,
                    SDS.terms.custom("DataDescription"),
                ),
            );
            this.bucket.write(writer);
        }
        writer.addQuads(quads);
    }
}

export class Extractor {
    extractor: CBDShapeExtractor;
    shapes: Shapes;
    lens: BasicLensM<Quad[], RecordDTO>;
    shape?: NBNode;

    bucket_cache: { [id: string]: Bucket } = {};

    constructor(extractor: CBDShapeExtractor, stream?: Term) {
        this.extractor = extractor;

        const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);
        this.shapes = extractShapes(quads, {
            "#Bucket": (item) => {
                return Bucket.parse(<BucketDTO>item, this.bucket_cache);
            },
        });

        this.lens = <BasicLensM<Quad[], RecordDTO>>match(
            undefined,
            SDS.terms.stream,
            stream,
        )
            .thenAll(subject)
            .thenSome(this.shapes.lenses["#Record"])
            .mapAll((x) => <RecordDTO>x);
    }

    async parse_records(quads: Quad[]): Promise<Record[]> {
        const store = RdfStore.createDefault();
        quads.forEach((quad) => store.addQuad(quad));

        const dtos = this.lens.execute(quads);

        return await Promise.all(
            dtos.map((dto) => {
                // This can be an apply like #Bucket
                return Record.parse(dto, store, this.bucket_cache);
            }),
        );
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
