import { afterEach, describe, expect, test } from "vitest";
import type { Term } from "@rdfjs/types";
import { DataFactory, Parser } from "n3";
import { extractShapes, setWarningHandler } from "rdf-lens";
import { TREE } from "@treecg/types";

import { Bucket, BucketRelation, Record } from "../lib/utils/index";
import {
    bucket_to_quads,
    Extractor,
    record_to_quads,
    relationToQuads,
    SHAPES_TEXT,
} from "../lib/utils/description";

const { namedNode, literal, quad } = DataFactory;

// Terms as "termType value", so terms of different factories compare equal
function plain(x: unknown): unknown {
    if (Array.isArray(x)) return x.map(plain);
    if (x && typeof x === "object") {
        if ("termType" in x) {
            return `${(<Term>x).termType} ${(<Term>x).value}`;
        }
        return Object.fromEntries(
            Object.entries(x).map(([k, v]) => [k, plain(v)]),
        );
    }
    return x;
}

describe("The SDS data description", () => {
    let restore: ReturnType<typeof setWarningHandler> | undefined;
    afterEach(() => {
        if (restore) setWarningHandler(restore);
        restore = undefined;
    });

    test("defines the record, bucket and relation shapes", () => {
        const shapes = extractShapes(
            new Parser({ baseIRI: "" }).parse(SHAPES_TEXT),
        );
        expect(Object.keys(shapes.lenses)).toEqual(
            expect.arrayContaining([
                "#Record",
                "#Bucket",
                "#Relation",
                "#RdfThing",
            ]),
        );
    });

    test("reads back what the bucketizer writes, without warnings", () => {
        const warnings: string[] = [];
        restore = setWarningHandler((message) => warnings.push(message));

        const stream = namedNode("http://example.org/stream");
        const root = new Bucket(namedNode("root"), [], true, false);
        const child = new Bucket(namedNode("root/a"), [], false, true);
        child.empty = true;
        const member = namedNode("http://example.org/member");
        const data = [
            quad(member, namedNode("http://example.org/p"), literal("a")),
        ];
        const relation: BucketRelation = {
            type: TREE.terms.EqualToRelation,
            target: child.id,
            value: literal("a"),
            path: { id: namedNode("http://example.org/p"), quads: [] },
        };
        const removed: BucketRelation = {
            type: TREE.terms.custom("GreaterThanRelation"),
            target: namedNode("root/old"),
        };

        const extract = new Extractor().extract_quads([
            ...record_to_quads(
                new Record({ id: member, quads: data }, stream),
                stream,
                [root, child],
            ),
            ...bucket_to_quads(root, stream),
            ...bucket_to_quads(child, stream),
            ...relationToQuads(root, relation),
            ...relationToQuads(root, removed, stream, true),
        ]);

        expect(plain(extract.getRecords())).toEqual([
            {
                stream: "NamedNode http://example.org/stream",
                payload: "NamedNode http://example.org/member",
                buckets: ["NamedNode root", "NamedNode root/a"],
            },
        ]);
        expect(plain(extract.getBuckets())).toEqual([
            {
                id: "NamedNode root",
                stream: "NamedNode http://example.org/stream",
                immutable: false,
                root: true,
            },
            {
                id: "NamedNode root/a",
                stream: "NamedNode http://example.org/stream",
                immutable: true,
                empty: true,
            },
        ]);
        expect(plain(extract.getRelations())).toEqual([
            {
                type: "NamedNode https://w3id.org/tree#EqualToRelation",
                stream: "NamedNode http://example.org/stream",
                origin: "NamedNode root",
                bucket: "NamedNode root/a",
                path: { id: "NamedNode http://example.org/p", quads: [] },
                value: { id: "Literal a", quads: [] },
            },
        ]);
        expect(plain(extract.getRemoveRelations())).toEqual([
            {
                type: "NamedNode https://w3id.org/tree#GreaterThanRelation",
                stream: "NamedNode http://example.org/stream",
                origin: "NamedNode root",
                bucket: "NamedNode root/old",
            },
        ]);
        expect(extract.getData()).toEqual(data);
        expect(warnings).toEqual([]);
    });

    test("keeps blank node payloads and paths as blank nodes", () => {
        const extract = new Extractor().extract(`
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

sds:DataDescription {
  [] sds:stream ex:stream;
    sds:bucket ex:bucket;
    sds:payload _:data.

  ex:bucket a sds:Bucket;
    sds:stream ex:stream;
    sds:relation _:rel.
  _:rel a sds:Relation;
    sds:relationType tree:GreaterThanRelation;
    sds:relationBucket ex:bucket2;
    sds:relationValue 1;
    sds:relationPath ( ex:x ex:y ).
}

_:data <x> 42; <y> 45.
`);

        const [record] = extract.getRecords();
        expect(record.payload.termType).toBe("BlankNode");
        expect(
            extract.getData().filter((q) => q.subject.equals(record.payload))
                .length,
        ).toBe(2);

        const [relation] = extract.getRelations();
        expect(relation.bucket.termType).toBe("NamedNode");
        expect(relation.path!.id.termType).toBe("BlankNode");
        expect(
            relation.path!.quads.some((q) =>
                q.subject.equals(relation.path!.id),
            ),
        ).toBe(true);
    });

    test("skips a record in a blank node bucket", () => {
        // A bucket is named after its IRI
        const extract = new Extractor().extract(`
@prefix sds: <https://w3id.org/sds#>.
sds:DataDescription {
    [] sds:payload <http://example.org/member>;
        sds:stream <http://example.org/stream>;
        sds:bucket _:bucket.
}
`);

        expect(extract.getRecords()).toEqual([]);
    });

    test("parses the records of its stream, with the data but not the removed relations", async () => {
        const quads = new Parser().parse(`
@prefix sds: <https://w3id.org/sds#>.
@prefix ex: <http://example.org/>.
sds:DataDescription {
    [] sds:payload ex:a; sds:stream ex:stream.
    [] sds:payload ex:b; sds:stream ex:other.
}
sds:RemoveDataDescription {
    ex:bucket sds:relation [ a sds:Relation ].
}
ex:a ex:p 1.
ex:b ex:p 2.
`);

        const records = await new Extractor(
            namedNode("http://example.org/stream"),
        ).parse_records(quads);

        expect(records.length).toBe(1);
        expect(records[0].data.id.value).toBe("http://example.org/a");
        expect(records[0].stream.value).toBe("http://example.org/stream");
        expect(records[0].data.quads.length).toBe(2);
        expect(
            records[0].data.quads.every(
                (q) => q.graph.termType === "DefaultGraph",
            ),
        ).toBe(true);
    });
});
