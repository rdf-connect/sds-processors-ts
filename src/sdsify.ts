import type { Stream, Writer } from "@rdfc/js-runner";
import { LDES, RDF, SDS, SHACL, XSD } from "@treecg/types";
import type { Quad, Term } from "@rdfjs/types";
import { blankNode } from "./core.js";
import {
    DataFactory,
    NamedNode,
    Parser,
    Quad_Object,
    Quad_Subject,
    Writer as NWriter,
} from "n3";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";
import { createHash } from "crypto";

function maybe_parse(data: Quad[] | string): Quad[] {
    if (typeof data === "string" || data instanceof String) {
        const parse = new Parser();
        return parse.parse(<string>data);
    } else {
        return data;
    }
}

async function getSubjects(
    store: RdfStore,
    pred?: Term,
    object?: Term,
    graph?: Term,
): Promise<Term[]> {
    const quads = await store.match(null, pred, object, graph).toArray();
    return quads.map((x) => x.subject);
}

async function getObjects(
    store: RdfStore,
    subject?: Term,
    pred?: Term,
    graph?: Term,
): Promise<Term[]> {
    const quads = await store.match(subject, pred, null, graph).toArray();
    return quads.map((x) => x.object);
}

// Find the main sh:NodeShape subject of a give Shape Graph.
// We determine this by assuming that the main node shape
// is not referenced by any other shape description.
// If more than one is found an exception is thrown.
async function extractMainNodeShape(store: RdfStore): Promise<Quad_Subject> {
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
                        "There are multiple main node shapes in a given shape. Unrelated shapes must be given as separate shape filters",
                    );
                }
            }
        }
        if (mainNodeShape) {
            return <Quad_Subject>mainNodeShape;
        } else {
            throw new Error("No main SHACL Node Shapes found in given shape filter");
        }
    } else {
        throw new Error("No SHACL Node Shapes found in given shape filter");
    }
}

export function sdsify(
    input: Stream<string | Quad[]>,
    output: Writer<string>,
    streamNode: Term,
    timestampPath?: Term,
    shapeFilters?: string[],
) {
    input.data(async (input) => {
        const dataStore = RdfStore.createDefault();
        maybe_parse(input).forEach((x) => dataStore.addQuad(x));
        console.log("[sdsify] Got input with", dataStore.size, "quads");
        const members: Array<{ subject: Term; quads: Quad[]; timestamp?: Term }> =
      [];
        const t0 = new Date();

        if (shapeFilters) {
            console.log("[sdsify] Extracting SDS members based on given shape(s)");

            for (const rawShape of shapeFilters) {
                const shapeStore = RdfStore.createDefault();
                new Parser().parse(rawShape).forEach((x) => shapeStore.addQuad(x));
                // Initialize shape extractor
                const shapeExtractor = new CBDShapeExtractor(shapeStore);
                // Find main node shape and target class
                const mainNodeShape = await extractMainNodeShape(shapeStore);
                const targetClasses = await getObjects(
                    shapeStore,
                    mainNodeShape,
                    SHACL.terms.targetClass,
                );
                const targetClass = targetClasses[0];

                if (!targetClass) {
                    throw new Error(
                        "Given main node shape does not define a sh:targetClass",
                    );
                }

                // Execute the CBDShapeExtractor over every targeted instance of the given shape
                const entities = await getSubjects(
                    dataStore,
                    RDF.terms.type,
                    targetClass,
                );
                for (const entity of entities) {
                    members.push({
                        subject: entity,
                        quads: await shapeExtractor.extract(
                            dataStore,
                            entity,
                            mainNodeShape,
                        ),
                    });
                }
            }
        } else {
            // Extract members based on a Concise Bound Description (CBD)
            const cbdExtractor = new CBDShapeExtractor();

            const subjects = await getSubjects(dataStore);
            const done = new Set<string>();
            for (const sub of subjects) {
                if (sub instanceof NamedNode) {
                    if (done.has(sub.value)) continue;
                    done.add(sub.value);
                    members.push({
                        subject: sub,
                        quads: await cbdExtractor.extract(dataStore, sub),
                    });
                }
            }
        }

        console.log(
            `[sdsify] Members extracted in ${new Date().getTime() - t0.getTime()} ms`,
        );

        // Sort members based on the given timestamp value (if any) to avoid out of order writing issues downstream
        if (timestampPath) {
            for (const member of members) {
                member.timestamp = (
                    await getObjects(dataStore, member.subject, timestampPath)
                )[0];
            }

            members.sort((a, b) => {
                const ta = new Date(a.timestamp!.value).getTime();
                const tb = new Date(b.timestamp!.value).getTime();
                return ta - tb;
            });
        }

        let membersCount = 0;

        // Create a unique transaction ID based on the data content and the current system time
        const hash = createHash("md5");
        const TRANSACTION_ID =
      hash
          .update(
              new NWriter().quadsToString(
                  dataStore.getQuads(null, null, null, null),
              ),
          )
          .digest("hex") +
      "_" +
      new Date().toISOString();

        for (const obj of members) {
            const quads = obj.quads;
            const blank = blankNode();

            quads.push(
                DataFactory.quad(blank, SDS.terms.payload, <Quad_Object>obj.subject),
                DataFactory.quad(blank, SDS.terms.stream, <Quad_Object>streamNode),
                // This is not standardized (yet)
                DataFactory.quad(
                    blank,
                    LDES.terms.custom("transactionId"),
                    DataFactory.literal(TRANSACTION_ID),
                ),
            );

            if (membersCount === Object.keys(members).length - 1) {
                // Annotate last member of a transaction
                quads.push(
                    // This is not standardized (yet)
                    DataFactory.quad(
                        blank,
                        LDES.terms.custom("isLastOfTransaction"),
                        DataFactory.literal("true", XSD.terms.custom("boolean")),
                    ),
                );
            }

            await output.push(new NWriter().quadsToString(quads));
            membersCount += 1;
        }
        // HEAD
        //
        console.log(
            `[sdsify] successfully pushed ${membersCount} members in ${
                new Date().getTime() - t0.getTime()
            } ms`,
        );
    //julianrojas87-master
    });

    input.on("end", async () => {
        console.log("[sdsify] input channel was closed down");
        await output.end();
    });
}