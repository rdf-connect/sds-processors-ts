import type { Quad, Quad_Object, Quad_Subject, Term } from "@rdfjs/types";
import { RDF, SDS, XSD } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import { Parser } from "n3";
import { extractShapes, match, subject } from "rdf-lens";
import { $INLINE_FILE } from "@ajuvercr/ts-transformer-inline-file";

import { Bucket, BucketRelation, RdfThing, Record } from "./index";

/**
 * The SDS data description: the records, buckets and relations the processors
 * write in the sds:DataDescription graph, next to the data they describe, and
 * the relations they remove in the sds:RemoveDataDescription graph. Both the
 * reading and the writing here follow configs/sds_shapes.ttl.
 */

const df = new DataFactory();

const DESCRIPTION = SDS.terms.custom("DataDescription");
const REMOVE_DESCRIPTION = SDS.terms.custom("RemoveDataDescription");
const TRUE = df.literal("true", XSD.terms.custom("boolean"));
const FALSE = df.literal("false", XSD.terms.custom("boolean"));

export const SHAPES_TEXT = $INLINE_FILE("../../configs/sds_shapes.ttl");

const Shapes = extractShapes(new Parser({ baseIRI: "" }).parse(SHAPES_TEXT));

export type SdsRecord = {
    stream: Term;
    payload: Term;
    buckets: Term[];
    dataless?: boolean;
};

export type SdsBucket = {
    id: Term;
    stream: Term;
    immutable?: boolean;
    root?: boolean;
    empty?: boolean;
};

export type SdsRelation = {
    type: Term;
    stream: Term;
    origin: Term;
    bucket: Term;
    path?: RdfThing;
    value?: RdfThing;
};

const RecordLens = match(undefined, SDS.terms.payload, undefined)
    .thenAll(subject)
    .thenSome(Shapes.lenses["#Record"]);

const BucketLens = match(undefined, RDF.terms.type, SDS.terms.custom("Bucket"))
    .thenAll(subject)
    .thenSome(Shapes.lenses["#Bucket"]);

const RelationLens = match(
    undefined,
    RDF.terms.type,
    SDS.terms.custom("Relation"),
)
    .thenAll(subject)
    .thenSome(Shapes.lenses["#Relation"]);

export class Extract {
    private data: Quad[] = [];
    private description: Quad[] = [];
    private removeDescription: Quad[] = [];

    constructor(full: Quad[]) {
        full.forEach((q) => {
            if (q.graph.equals(DESCRIPTION)) {
                this.description.push(q);
            } else if (q.graph.equals(REMOVE_DESCRIPTION)) {
                this.removeDescription.push(q);
            } else {
                this.data.push(q);
            }
        });
    }

    /** The quads outside the description graphs: the data of the records. */
    getData(): Quad[] {
        return this.data;
    }

    /** The records, only those of `stream` when it is given. */
    getRecords(stream?: Term): SdsRecord[] {
        const records = <SdsRecord[]>RecordLens.execute(this.description);
        return stream
            ? records.filter((record) => record.stream.equals(stream))
            : records;
    }

    getBuckets(): SdsBucket[] {
        return <SdsBucket[]>BucketLens.execute(this.description);
    }

    getRelations(): SdsRelation[] {
        return <SdsRelation[]>RelationLens.execute(this.description);
    }

    getRemoveRelations(): SdsRelation[] {
        return <SdsRelation[]>RelationLens.execute(this.removeDescription);
    }
}

export class Extractor {
    // The stream whose records parse_records returns, every stream when undefined.
    stream?: Term;

    constructor(stream?: Term) {
        this.stream = stream;
    }

    extract_quads(quads: Quad[]): Extract {
        return new Extract(quads);
    }

    extract(inp: string): Extract {
        return new Extract(new Parser().parse(inp));
    }

    /** The records of the stream, each with the data of the whole message. */
    async parse_records(quads: Quad[]): Promise<Record[]> {
        const extract = this.extract_quads(quads);
        const data = extract.getData();
        return extract
            .getRecords(this.stream)
            .map(
                (record) =>
                    new Record(
                        { id: record.payload, quads: data },
                        record.stream,
                    ),
            );
    }
}

export function record_to_quads(
    record: Record,
    resultingStream: Term,
    buckets: Bucket[],
): Quad[] {
    const id = df.blankNode();
    const out: Quad[] = [
        df.quad(
            id,
            SDS.terms.payload,
            <Quad_Object>record.data.id,
            DESCRIPTION,
        ),
        df.quad(
            id,
            SDS.terms.stream,
            <Quad_Object>resultingStream,
            DESCRIPTION,
        ),
        ...buckets
            .map((bucket) => bucket.id)
            .map((bucket) =>
                df.quad(id, SDS.terms.bucket, <Quad_Object>bucket, DESCRIPTION),
            ),
        ...(record.data.quads.length
            ? record.data.quads
            : [df.quad(id, SDS.terms.custom("dataless"), TRUE, DESCRIPTION)]),
    ];
    return out;
}

export function bucket_to_quads(bucket: Bucket, stream?: Term): Quad[] {
    const out: Quad[] = [
        df.quad(
            <Quad_Subject>bucket.id,
            RDF.terms.type,
            SDS.terms.custom("Bucket"),
            DESCRIPTION,
        ),
    ];
    out.push(
        df.quad(
            <Quad_Subject>bucket.id,
            SDS.terms.custom("immutable"),
            bucket.immutable ? TRUE : FALSE,
            DESCRIPTION,
        ),
    );

    if (bucket.root) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.custom("isRoot"),
                TRUE,
                DESCRIPTION,
            ),
        );
    }

    // Send empty triples for the bucket if it is set as true.
    if (bucket.empty) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.custom("empty"),
                TRUE,
                DESCRIPTION,
            ),
        );
        bucket.empty = false;
    }

    if (stream) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.stream,
                <Quad_Object>stream,
                DESCRIPTION,
            ),
        );
    }

    return out;
}

export function relationToQuads(
    bucket: Bucket,
    rel: BucketRelation,
    stream?: Term,
    remove?: boolean,
): Quad[] {
    const graph = remove ? REMOVE_DESCRIPTION : DESCRIPTION;
    const out: Quad[] = [];
    const id = df.blankNode();
    out.push(
        df.quad(<Quad_Subject>bucket.id, SDS.terms.relation, id, graph),
        df.quad(
            id,
            RDF.terms.type,
            <Quad_Object>SDS.terms.custom("Relation"),
            graph,
        ),
        df.quad(id, SDS.terms.relationType, <Quad_Object>rel.type, graph),
        df.quad(id, SDS.terms.relationBucket, <Quad_Object>rel.target, graph),
    );

    if (rel.path) {
        out.push(
            df.quad(
                id,
                SDS.terms.relationPath,
                <Quad_Object>rel.path.id,
                graph,
            ),
            ...rel.path.quads.map((x) =>
                df.quad(x.subject, x.predicate, x.object, graph),
            ),
        );
    }

    if (rel.value) {
        out.push(
            df.quad(id, SDS.terms.relationValue, <Quad_Object>rel.value, graph),
        );
    }

    if (stream) {
        out.push(
            df.quad(
                <Quad_Subject>bucket.id,
                SDS.terms.stream,
                <Quad_Object>stream,
                graph,
            ),
        );
    }

    return out;
}
