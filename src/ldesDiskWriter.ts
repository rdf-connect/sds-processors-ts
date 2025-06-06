import { Quad, Quad_Object } from "@rdfjs/types";
import { Stream } from "@rdfc/js-runner";
import { maybeParse } from "./utils";
import { getLoggerFor } from "./utils/logUtil";
import { Extractor } from "./utils/extractor";
import path from "node:path";
import { DataFactory } from "rdf-data-factory";
import { DC, LDES, RDF, SDS, TREE, XSD } from "@treecg/types";
import { Parser, Writer } from "n3";
import * as fs from "node:fs";

const logger = getLoggerFor("ldesDiskWriter");
const df = new DataFactory();

// for an i, add /0/1/2/3/.../i to the base URI
const INTERNAL_TEMP_BASE_HOST = "http://ldes-disk-writer.internal/";
const INTERNAL_TEMP_BASE_URI =
    INTERNAL_TEMP_BASE_HOST +
    Array.from({ length: 20 }, (_, i) => i)
        .map((i) => i.toString())
        .join("/") +
    "/";

export function ldesDiskWriter(
    data: Stream<string | Quad[]>,
    metadata: Stream<string | Quad[]>,
    directory: string,
): () => Promise<void> {
    const extractor = new Extractor();

    metadata.data(async (input: string | Quad[]) => {
        const metadataQuads = maybeParse(input);

        logger.debug(`[metadata] Got input with ${metadataQuads.length} quads`);

        const streams = metadataQuads
            .filter(
                (q) =>
                    q.predicate.equals(RDF.terms.type) &&
                    q.object.equals(SDS.terms.Stream),
            )
            .map((q) => q.subject);

        metadataQuads.push(
            df.quad(
                df.namedNode("index.trig"),
                RDF.terms.type,
                LDES.terms.EventStream,
            ),
        );

        const datasetId = metadataQuads.find(
            (q) =>
                streams.map((q) => q.value).includes(q.subject.value) &&
                q.predicate.equals(SDS.terms.dataset),
        )?.object;
        if (datasetId) {
            metadataQuads
                .filter((q) => q.subject.equals(datasetId))
                .forEach((q) =>
                    metadataQuads.push(
                        df.quad(
                            df.namedNode("index.trig"),
                            q.predicate,
                            q.object,
                        ),
                    ),
                );
        }
        const shapeId = metadataQuads.find(
            (q) =>
                streams.map((q) => q.value).includes(q.subject.value) &&
                q.predicate.equals(SDS.terms.shape),
        )?.object;
        if (shapeId) {
            metadataQuads.push(
                df.quad(df.namedNode("index.trig"), TREE.terms.shape, shapeId),
            );
        }

        // Add information about the different views (defined by the streams) in the LDES.
        for (const stream of streams) {
            const viewId = df.namedNode(
                path.join(encodePathValue(stream.value), "index.trig"),
            );
            metadataQuads.push(
                df.quad(df.namedNode("index.trig"), TREE.terms.view, viewId),
            );
            metadataQuads.push(
                df.quad(viewId, RDF.terms.type, TREE.terms.Node),
            );
            const viewDescriptionId = df.namedNode(
                `${viewId.value}#description`,
            );
            metadataQuads.push(
                df.quad(
                    viewId,
                    TREE.terms.custom("viewDescription"),
                    viewDescriptionId,
                ),
            );
            metadataQuads.push(
                df.quad(
                    viewDescriptionId,
                    RDF.terms.type,
                    TREE.terms.custom("ViewDescription"),
                ),
            );
            metadataQuads.push(
                df.quad(
                    viewDescriptionId,
                    DC.terms.custom("endpointURL"),
                    viewId,
                ),
            );
            metadataQuads.push(
                df.quad(
                    viewDescriptionId,
                    DC.terms.custom("servesDataset"),
                    df.namedNode("index.trig"),
                ),
            );
            metadataQuads.push(
                df.quad(
                    viewDescriptionId,
                    LDES.terms.custom("managedBy"),
                    stream,
                ),
            );

            // Create the directory and index file for the view
            const viewPath = path.join(
                directory,
                encodePathValue(stream.value),
            );
            await fs.promises.mkdir(viewPath, { recursive: true });

            const viewIndexPath = path.join(viewPath, "index.trig");
            if (!fs.existsSync(viewIndexPath)) {
                const viewQuads = [
                    df.quad(
                        df.namedNode("../index.trig"),
                        RDF.terms.type,
                        LDES.terms.EventStream,
                    ),
                    df.quad(
                        df.namedNode("index.trig"),
                        RDF.terms.type,
                        TREE.terms.Node,
                    ),
                    df.quad(
                        df.namedNode("index.trig"),
                        DC.terms.custom("isPartOf"),
                        df.namedNode("../index.trig"),
                    ),
                ];
                const data = await quadsToString(viewQuads);
                await fs.promises.writeFile(viewIndexPath, data);
            }
        }

        // Always writing the metadata makes sure the metadata is always up-to-date.
        const metadataString = await quadsToString(metadataQuads);
        await fs.promises.writeFile(
            path.join(directory, "index.trig"),
            metadataString,
        );
    });

    data.data(async (input: string | Quad[]) => {
        const data = maybeParse(input);
        const extract = extractor.extract_quads(data);

        logger.debug(`[data] Got input with ${data.length} quads`);

        const members: Map<string, Quad[]> = new Map<string, Quad[]>(); // member id -> quads

        // For each bucket, make sure it's directory and index file containing the metadata exists
        // And remove all members if the bucket needs to be emptied
        for (const bucket of extract.getBuckets()) {
            const bucketPath = path.join(
                directory,
                encodePathValue(bucket.streamId),
                encodePathValue(bucket.id, true),
            );

            // Make sure bucket directory exists
            await fs.promises.mkdir(bucketPath, { recursive: true });

            // If bucket index.trig does not exist, create it with minimal metadata
            const bucketIndexPath = path.join(bucketPath, "index.trig");
            if (!fs.existsSync(bucketIndexPath)) {
                const relativeLdesId = df.namedNode(
                    path.join(
                        path.relative(
                            path.join(
                                encodePathValue(bucket.streamId),
                                encodePathValue(bucket.id, true),
                            ),
                            "",
                        ),
                        "index.trig",
                    ),
                );
                const metadataQuads = [
                    df.quad(
                        relativeLdesId,
                        RDF.terms.type,
                        LDES.terms.EventStream,
                    ),
                    df.quad(
                        df.namedNode("index.trig"),
                        RDF.terms.type,
                        TREE.terms.Node,
                    ),
                    df.quad(
                        df.namedNode("index.trig"),
                        DC.terms.custom("isPartOf"),
                        relativeLdesId,
                    ),
                ];
                const data = await quadsToString(metadataQuads);
                await fs.promises.writeFile(bucketIndexPath, data);
            }

            if (bucket.empty) {
                logger.debug(`[data] Emptying bucket ${bucket.id}`);
                // Go over quads, group them by metadata, members, and relations.
                const content = await fs.promises.readFile(bucketIndexPath);
                const quads = new Parser({
                    baseIRI: INTERNAL_TEMP_BASE_URI,
                }).parse(content.toString());

                const metadata = [];
                let memberId: Quad_Object | undefined = undefined;
                let memberQuads: Quad[] = [];
                const relations = [];

                const handleFullMember = () => {
                    if (memberId) {
                        members.set(memberId.value, memberQuads);
                        memberQuads = [];
                        memberId = undefined;
                    }
                };

                let handlingMembers = false;
                let handlingRelations = false;
                for (const quad of quads) {
                    if (quad.predicate.equals(TREE.terms.member)) {
                        handlingMembers = true;
                        handlingRelations = false;
                        handleFullMember();
                    } else if (quad.predicate.equals(TREE.terms.relation)) {
                        handlingRelations = true;
                        handlingMembers = false;
                        handleFullMember();
                    }
                    if (handlingMembers) {
                        if (quad.predicate.equals(TREE.terms.member)) {
                            memberId = quad.object;
                        } else {
                            memberQuads.push(quad);
                        }
                    } else if (handlingRelations) {
                        relations.push(quad);
                    } else {
                        metadata.push(quad);
                    }
                }
                handleFullMember();

                // We needed to empty bucket, so only add metadata and relations back to the file
                const data = await quadsToString([...metadata, ...relations]);
                await fs.promises.writeFile(bucketIndexPath, data);
            }

            if (bucket.root) {
                // Check if a relation to the viewId from the bucketId already exists
                const viewIndexPath = path.join(
                    directory,
                    encodePathValue(bucket.streamId),
                    "index.trig",
                );
                const content = await fs.promises.readFile(viewIndexPath);
                const existingQuads = new Parser({
                    baseIRI: INTERNAL_TEMP_BASE_URI,
                }).parse(content.toString());
                const absoluteRelativeBucketId = df.namedNode(
                    new URL(
                        path.join(
                            encodePathValue(bucket.id, true),
                            "index.trig",
                        ),
                        INTERNAL_TEMP_BASE_URI,
                    ).href,
                );
                if (
                    !existingQuads.some(
                        (q) =>
                            q.object.equals(absoluteRelativeBucketId) &&
                            q.predicate.equals(TREE.terms.node),
                    )
                ) {
                    const bn = df.blankNode();
                    const quads = [
                        df.quad(
                            df.namedNode("index.trig"),
                            TREE.terms.relation,
                            bn,
                        ),
                        df.quad(bn, RDF.terms.type, TREE.terms.Relation),
                        df.quad(bn, TREE.terms.node, absoluteRelativeBucketId),
                    ];
                    const data = await quadsToString(quads);
                    await fs.promises.appendFile(viewIndexPath, data);
                }
            }

            if (bucket.immutable) {
                // Check if the bucket is already immutable, otherwise set it to immutable
                const content = await fs.promises.readFile(bucketIndexPath);
                const quads = new Parser({
                    baseIRI: INTERNAL_TEMP_BASE_URI,
                }).parse(content.toString());
                const immutable = quads.some(
                    (q) =>
                        q.predicate.equals(LDES.terms.custom("immutable")) &&
                        q.object.equals(
                            df.literal("true", XSD.terms.custom("boolean")),
                        ),
                );
                if (!immutable) {
                    const quads = [
                        df.quad(
                            df.namedNode("index.trig"),
                            LDES.terms.custom("immutable"),
                            df.literal("true", XSD.terms.custom("boolean")),
                        ),
                    ];
                    const data = await quadsToString(quads);
                    await fs.promises.appendFile(bucketIndexPath, data);
                }
            }
        }

        // For each record, append the member contents to the file corresponding to the bucket
        for (const record of extract.getRecords()) {
            for (const bucket of record.buckets) {
                // Append the member contents to the file corresponding to the bucket
                const bucketIndexPath = path.join(
                    directory,
                    encodePathValue(record.stream),
                    encodePathValue(bucket, true),
                    "index.trig",
                );

                const relativeLdesId = df.namedNode(
                    path.join(
                        path.relative(
                            path.join(
                                encodePathValue(record.stream),
                                encodePathValue(bucket, true),
                            ),
                            "",
                        ),
                        "index.trig",
                    ),
                );
                const quads = [
                    df.quad(
                        relativeLdesId,
                        TREE.terms.member,
                        df.namedNode(encodePathValue(record.payload, true)),
                    ),
                ];
                if (!record.dataless) {
                    quads.push(...extract.getData());
                } else {
                    quads.push(
                        ...(members.get(
                            new URL(record.payload, INTERNAL_TEMP_BASE_URI)
                                .href,
                        ) || []),
                    );
                }
                const data = await quadsToString(quads);

                await fs.promises.appendFile(bucketIndexPath, data);
            }
        }

        for (const relation of extract.getRemoveRelations()) {
            // Remove the relation from the file corresponding to the bucket
            const bucketIndexPath = path.join(
                directory,
                encodePathValue(relation.stream),
                encodePathValue(relation.origin, true),
                "index.trig",
            );

            const content = await fs.promises.readFile(bucketIndexPath);
            const quads = new Parser({ baseIRI: INTERNAL_TEMP_BASE_URI }).parse(
                content.toString(),
            );
            // Find blankNode subject of the relation and remove all related quads
            const relationTargetIndexPath = path.join(
                encodePathValue(
                    path.relative(relation.origin, relation.bucket),
                    true,
                ),
                "index.trig",
            );
            const blankNode = quads.find(
                (bnQ) =>
                    bnQ.predicate.equals(RDF.terms.type) &&
                    bnQ.object.value === relation.type &&
                    quads.some(
                        (q) =>
                            q.subject.equals(bnQ.subject) &&
                            q.predicate.equals(TREE.terms.node) &&
                            q.object.equals(
                                df.namedNode(
                                    new URL(
                                        relationTargetIndexPath,
                                        INTERNAL_TEMP_BASE_URI,
                                    ).href,
                                ),
                            ),
                    ) &&
                    (!relation.path ||
                        quads.some(
                            (q) =>
                                q.subject.equals(bnQ.subject) &&
                                q.predicate.equals(TREE.terms.path) &&
                                q.object.equals(relation.path!.id),
                        )) &&
                    (!relation.value ||
                        quads.some(
                            (q) =>
                                q.subject.equals(bnQ.subject) &&
                                q.predicate.equals(TREE.terms.value) &&
                                q.object.equals(relation.value!.id),
                        )),
            )?.subject;
            if (!blankNode) {
                logger.error(
                    `Could not find blankNode to remove relation ${relation.type} from ${relation.origin} to ${relation.bucket}`,
                );
                continue;
            }
            const updatedQuads = quads.filter(
                (q) =>
                    !q.subject.equals(blankNode) && !q.object.equals(blankNode),
            );
            const data = await quadsToString(updatedQuads);

            await fs.promises.writeFile(bucketIndexPath, data);
        }

        for (const relation of extract.getRelations()) {
            // Append the relation to the file corresponding to the bucket
            const bucketIndexPath = path.join(
                directory,
                encodePathValue(relation.stream),
                encodePathValue(relation.origin, true),
                "index.trig",
            );

            const bn = df.blankNode();
            const quads = [
                df.quad(df.namedNode("index.trig"), TREE.terms.relation, bn),
                df.quad(bn, RDF.terms.type, df.namedNode(relation.type)),
                df.quad(
                    bn,
                    TREE.terms.node,
                    df.namedNode(
                        path.join(
                            encodePathValue(
                                path.relative(relation.origin, relation.bucket),
                                true,
                            ),
                            "index.trig",
                        ),
                    ),
                ),
            ];
            if (relation.path) {
                quads.push(
                    df.quad(bn, TREE.terms.path, <Quad_Object>relation.path.id),
                );
                quads.push(...relation.path.quads);
            }
            if (relation.value) {
                quads.push(
                    df.quad(
                        bn,
                        TREE.terms.value,
                        <Quad_Object>relation.value.id,
                    ),
                );
                quads.push(...relation.value.quads);
            }
            const data = await quadsToString(quads);

            await fs.promises.appendFile(bucketIndexPath, data);
        }
    });

    return async () => {};
}

async function quadsToString(quads: Quad[]) {
    const writer = new Writer({
        format: "application/trig",
        baseIRI: INTERNAL_TEMP_BASE_URI,
    });
    writer.addQuads(quads);
    return await new Promise<string>((resolve, reject) => {
        writer.end((error, result) => {
            if (error) {
                reject(error);
            } else {
                resolve(result);
            }
        });
    });
}

function encodePathValue(value: string, alreadyUriEncoded = false): string {
    if (!alreadyUriEncoded) {
        value = encodeURIComponent(value);
    }
    // Replace % with _ to avoid issues with simple HTTP servers like GH Pages, probably trying to wrongly decode the URI component.
    return value.replace(/%/g, "_");
}
