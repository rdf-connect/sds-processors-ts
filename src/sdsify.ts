import type { Stream, Writer } from "@rdfc/js-runner";
import { LDES, RDF, SDS, SHACL, XSD } from "@treecg/types";
import type { Quad, Term, Quad_Object, Quad_Subject } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { getSubjects } from "./utils/index";
import { Parser, Writer as NWriter } from "n3";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";
import { createHash } from "crypto";

const df = new DataFactory();

type SDSMember = {
    quads: Quad[];
    timestamp?: Term;
};

function maybeParse(data: Quad[] | string): Quad[] {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        return parse.parse(<string>data);
    } else {
        return data;
    }
}

// Find the main sh:NodeShape subject of a give Shape Graph.
// We determine this by assuming that the main node shape
// is not referenced by any other shape description.
// If more than one is found an exception is thrown.
async function extractMainNodeShape(
    store: RdfStore | null,
): Promise<Quad_Subject | undefined> {
    if (store) {
        const nodeShapes = await getSubjects(
            store,
            RDF.terms.type,
            SHACL.terms.NodeShape,
        );
        let mainNodeShape = null;

        if (nodeShapes && nodeShapes.length > 0) {
            for (const ns of nodeShapes) {
                const referenced = await getSubjects(store, undefined, ns);
                const isNotReferenced = referenced.length === 0;

                if (isNotReferenced) {
                    if (!mainNodeShape) {
                        mainNodeShape = ns;
                    } else {
                        throw new Error(
                            "There are multiple main node shapes in a given shape. " +
                                "Use sh:xone or sh:or to provide multiple unrelated shapes together.",
                        );
                    }
                }
            }
            if (mainNodeShape) {
                return <Quad_Subject>mainNodeShape;
            } else {
                throw new Error(
                    "No main SHACL Node Shapes found in given shape filter",
                );
            }
        } else {
            throw new Error("No SHACL Node Shapes found in given shape filter");
        }
    }
}

function getExtractor(shapeStore: RdfStore | null): CBDShapeExtractor {
    if (!shapeStore) {
        return new CBDShapeExtractor();
    } else {
        return new CBDShapeExtractor(shapeStore, undefined, {
            fetch: async () =>
                new Response("", {
                    headers: { "content-type": "text/turtle" },
                }),
        });
    }
}

export function sdsify(
    input: Stream<string | Quad[]>,
    output: Writer<string>,
    streamNode: Term,
    types?: Term[],
    timestampPath?: Term,
    shape?: string,
) {
    // Setup member extractor
    const shapeStore = shape ? RdfStore.createDefault() : null;
    if (shape) {
        maybeParse(shape).forEach((x) => {
            if (shapeStore) shapeStore.addQuad(x);
        });
    }
    const extractor = getExtractor(shapeStore);

    input.data(async (input) => {
        const dataStore = RdfStore.createDefault();
        maybeParse(input).forEach((x) => dataStore.addQuad(x));
        console.log("[sdsify] Got input with", dataStore.size, "quads");

        const members: { [id: string]: SDSMember } = {};
        const t0 = new Date();
        // Get shape Id (if any)
        const shapeId = shape
            ? await extractMainNodeShape(shapeStore)
            : undefined;
        const subjects = [];

        if (types?.length) {
            for (const t of types) {
                // Group quads based on given member type
                subjects.push(
                    ...(await getSubjects(dataStore, RDF.terms.type, t)),
                );
            }
        } else {
            subjects.push(...(await getSubjects(dataStore)));
        }

        // Extract members from received quads
        await Promise.all(
            subjects.map(async (subject) => {
                if (
                    subject.termType === "NamedNode" &&
                    !members[subject.value]
                ) {
                    const membQuads = await extractor.extract(
                        dataStore,
                        subject,
                        shapeId,
                    );
                    members[subject.value] = {
                        quads: membQuads,
                        timestamp: timestampPath
                            ? dataStore.getQuads(subject, timestampPath)[0]
                                  .object
                            : undefined,
                    };
                }
            }),
        );

        console.log(
            `[sdsify] Members extracted in ${new Date().getTime() - t0.getTime()} ms`,
        );

        // Sort members based on the given timestamp value (if any) to avoid out of order writing issues downstream
        const orderedMembersIds = Object.keys(members);
        if (timestampPath) {
            orderedMembersIds.sort((a, b) => {
                const ta = new Date(members[a].timestamp!.value).getTime();
                const tb = new Date(members[b].timestamp!.value).getTime();
                return ta - tb;
            });
        }
        let membersCount = 0;

        // Create a unique transaction ID based on the data content and the current system time
        const hash = createHash("md5");
        const TRANSACTION_ID =
            hash
                .update(new NWriter().quadsToString(dataStore.getQuads()))
                .digest("hex") +
            "_" +
            new Date().toISOString();

        for (const sub of orderedMembersIds) {
            const quads = members[sub].quads;
            const blank = df.blankNode();

            quads.push(
                df.quad(
                    blank,
                    SDS.terms.payload,
                    <Quad_Object>df.namedNode(sub),
                    SDS.terms.custom("DataDescription"),
                ),
                df.quad(
                    blank,
                    SDS.terms.stream,
                    <Quad_Object>streamNode,
                    SDS.terms.custom("DataDescription"),
                ),
                // This is not standardized (yet)
                df.quad(
                    blank,
                    LDES.terms.custom("transactionId"),
                    df.literal(TRANSACTION_ID),
                    SDS.terms.custom("DataDescription"),
                ),
            );

            if (membersCount === Object.keys(members).length - 1) {
                // Annotate last member of a transaction
                quads.push(
                    // This is not standardized (yet)
                    df.quad(
                        blank,
                        LDES.terms.custom("isLastOfTransaction"),
                        df.literal("true", XSD.terms.custom("boolean")),
                        SDS.terms.custom("DataDescription"),
                    ),
                );
            }

            await output.push(new NWriter().quadsToString(quads));
            membersCount += 1;
        }

        console.log(
            `[sdsify] successfully pushed ${membersCount} members in ${new Date().getTime() - t0.getTime()} ms`,
        );
    });

    input.on("end", async () => {
        console.log("[sdsify] input channel was closed down");
        await output.end();
    });
}
