import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { LDES, RDF, SDS, SHACL, XSD } from "@treecg/types";
import type { Quad, Quad_Object, Quad_Subject, Term } from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";
import { getSubjects, maybeParse } from "./utils/index";
import { Parser, Writer as NWriter } from "n3";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";
import { createHash } from "crypto";
import { getLoggerFor } from "./utils/logUtil";

const logger = getLoggerFor("sdsify");

const df = new DataFactory();

type SDSMember = {
    quads: Quad[];
    timestamp?: Term;
};

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

type Args = {
    input: Reader;
    output: Writer;
    streamNode: Term;
    types?: Term[];
    timestampPath?: Term;
    shape?: string;
};
export class Sdsify extends Processor<Args> {
    extractor: CBDShapeExtractor;
    shapeId: Term | undefined;
    async init(this: Args & this): Promise<void> {
        // Setup member extractor
        const shapeStore = this.shape ? RdfStore.createDefault() : null;
        if (this.shape) {
            maybeParse(this.shape).forEach((x) => {
                if (shapeStore) shapeStore.addQuad(x);
            });
        }
        this.extractor = getExtractor(shapeStore);
        this.shapeId = this.shape
            ? await extractMainNodeShape(shapeStore)
            : undefined;
    }
    async transform(this: Args & this): Promise<void> {
        for await (const input of this.input.strings()) {
            const dataStore = RdfStore.createDefault();
            maybeParse(input).forEach((x) => dataStore.addQuad(x));
            logger.debug(`Got input with ${dataStore.size} quads`);

            const members: { [id: string]: SDSMember } = {};
            const t0 = new Date();
            // Get shape Id (if any)

            const subjects = [];

            if (this.types?.length) {
                for (const t of this.types) {
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
                        const membQuads = await this.extractor.extract(
                            dataStore,
                            subject,
                            this.shapeId,
                        );
                        members[subject.value] = {
                            quads: membQuads,
                            timestamp: this.timestampPath
                                ? dataStore.getQuads(
                                      subject,
                                      this.timestampPath,
                                  )[0].object
                                : undefined,
                        };
                    }
                }),
            );

            logger.debug(
                `Members extracted in ${new Date().getTime() - t0.getTime()} ms`,
            );

            // Sort members based on the given timestamp value (if any) to avoid out of order writing issues downstream
            const orderedMembersIds = Object.keys(members);
            if (this.timestampPath) {
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
                        <Quad_Object>this.streamNode,
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

                await this.output.string(new NWriter().quadsToString(quads));
                membersCount += 1;
            }

            logger.debug(
                `Successfully pushed ${membersCount} members in ${
                    new Date().getTime() - t0.getTime()
                } ms`,
            );
        }

        logger.info("Input channel was closed down");
        await this.output.close();
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }
}
