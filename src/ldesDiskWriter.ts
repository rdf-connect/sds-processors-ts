import { NamedNode, Quad, Quad_Object } from "@rdfjs/types";
import { Stream } from "@rdfc/js-runner";
import { maybeParse } from "./utils";
import { getLoggerFor } from "./utils/logUtil";
import { Extractor } from "./utils/extractor";
import path from "node:path";
import { DataFactory } from "rdf-data-factory";
import { DC, LDES, RDF, SDS, TREE } from "@treecg/types";
import { Parser, Writer } from "n3";
import * as fs from "node:fs";

const logger = getLoggerFor("ldesDiskWriter");
const df = new DataFactory();

export function ldesDiskWriter(
    data: Stream<string | Quad[]>,
    metadata: Stream<string | Quad[]>,
    directory: string,
    ldesId: NamedNode,
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
            df.quad(ldesId, RDF.terms.type, LDES.terms.EventStream),
        );

        const datasetId = metadataQuads.find(
            (q) =>
                streams.includes(q.subject) &&
                q.predicate.equals(SDS.terms.dataset),
        )?.object;
        if (datasetId) {
            metadataQuads
                .filter((q) => q.subject.equals(datasetId))
                .forEach((q) =>
                    metadataQuads.push(df.quad(ldesId, q.predicate, q.object)),
                );
        }

        // Add information about the different views (defined by the streams) in the LDES.
        for (const stream of streams) {
            const viewId = df.namedNode(
                path.posix
                    .join(ldesId.value, encodeURIComponent(stream.value))
                    .replace(":/", "://"),
            );
            metadataQuads.push(df.quad(ldesId, TREE.terms.view, viewId));
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
                    ldesId,
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
                encodeURIComponent(stream.value),
            );
            await fs.promises.mkdir(viewPath, { recursive: true });

            const viewIndexPath = path.join(viewPath, "index.ttl");
            if (!fs.existsSync(viewIndexPath)) {
                const viewQuads = [
                    df.quad(ldesId, RDF.terms.type, LDES.terms.EventStream),
                    df.quad(viewId, RDF.terms.type, TREE.terms.Node),
                    df.quad(viewId, DC.terms.custom("isPartOf"), ldesId),
                ];
                const data = new Writer().quadsToString(viewQuads);
                await fs.promises.writeFile(viewIndexPath, data);
            }
        }

        // Always writing the metadata makes sure the metadata is always up-to-date.
        const metadataString = new Writer().quadsToString(metadataQuads);
        await fs.promises.writeFile(
            path.join(directory, "index.ttl"),
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
                encodeURIComponent(bucket.streamId),
                bucket.id,
            );
            const bucketId = df.namedNode(
                path.posix
                    .join(
                        ldesId.value,
                        encodeURIComponent(bucket.streamId),
                        bucket.id,
                    )
                    .replace(":/", "://"),
            );

            // Make sure bucket directory exists
            await fs.promises.mkdir(bucketPath, { recursive: true });

            // If bucket index.ttl does not exist, create it with minimal metadata
            const bucketIndexPath = path.join(bucketPath, "index.ttl");
            if (!fs.existsSync(bucketIndexPath)) {
                const metadataQuads = [
                    df.quad(ldesId, RDF.terms.type, LDES.terms.EventStream),
                    df.quad(bucketId, RDF.terms.type, TREE.terms.Node),
                    df.quad(bucketId, DC.terms.custom("isPartOf"), ldesId),
                ];
                const data = new Writer().quadsToString(metadataQuads);
                await fs.promises.writeFile(bucketIndexPath, data);
            }

            if (bucket.empty) {
                logger.debug(`[data] Emptying bucket ${bucket.id}`);
                // Go over quads, group them by metadata, members, and relations.
                const content = await fs.promises.readFile(bucketIndexPath);
                const quads = new Parser().parse(content.toString());

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
                const data = new Writer().quadsToString([
                    ...metadata,
                    ...relations,
                ]);
                await fs.promises.writeFile(bucketIndexPath, data);
            }

            if (bucket.root) {
                // Check if a relation to the viewId from the bucketId already exists
                const viewIndexPath = path.join(
                    directory,
                    encodeURIComponent(bucket.streamId),
                    "index.ttl",
                );
                const content = await fs.promises.readFile(viewIndexPath);
                const existingQuads = new Parser().parse(content.toString());
                if (
                    !existingQuads.some(
                        (q) =>
                            q.object.equals(bucketId) &&
                            q.predicate.equals(TREE.terms.node),
                    )
                ) {
                    const bn = df.blankNode();
                    const quads = [
                        df.quad(
                            df.namedNode(
                                path.posix
                                    .join(
                                        ldesId.value,
                                        encodeURIComponent(bucket.streamId),
                                    )
                                    .replace(":/", "://"),
                            ),
                            TREE.terms.relation,
                            bn,
                        ),
                        df.quad(bn, RDF.terms.type, TREE.terms.Relation),
                        df.quad(bn, TREE.terms.node, bucketId),
                    ];
                    const data = new Writer().quadsToString(quads);
                    await fs.promises.appendFile(viewIndexPath, data);
                }
            }
        }

        // For each record, append the member contents to the file corresponding to the bucket
        for (const record of extract.getRecords()) {
            for (const bucket of record.buckets) {
                // Append the member contents to the file corresponding to the bucket
                const bucketIndexPath = path.join(
                    directory,
                    encodeURIComponent(record.stream),
                    bucket,
                    "index.ttl",
                );

                const quads = [
                    df.quad(
                        ldesId,
                        TREE.terms.member,
                        df.namedNode(record.payload),
                    ),
                ];
                if (!record.dataless) {
                    quads.push(...extract.getData());
                } else {
                    quads.push(...(members.get(record.payload) || []));
                }
                const data = new Writer().quadsToString(quads);

                await fs.promises.appendFile(bucketIndexPath, data);
            }
        }

        for (const relation of extract.getRelations()) {
            // Append the relation to the file corresponding to the bucket
            const bucketIndexPath = path.join(
                directory,
                encodeURIComponent(relation.stream),
                relation.origin,
                "index.ttl",
            );

            const bn = df.blankNode();
            const quads = [
                df.quad(
                    df.namedNode(
                        path.posix
                            .join(
                                ldesId.value,
                                encodeURIComponent(relation.stream),
                                relation.origin,
                            )
                            .replace(":/", "://"),
                    ),
                    TREE.terms.relation,
                    bn,
                ),
                df.quad(bn, RDF.terms.type, df.namedNode(relation.type)),
                df.quad(
                    bn,
                    TREE.terms.node,
                    df.namedNode(
                        path.posix
                            .join(
                                ldesId.value,
                                encodeURIComponent(relation.stream),
                                relation.bucket,
                            )
                            .replace(":/", "://"),
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
            const data = new Writer().quadsToString(quads);

            await fs.promises.appendFile(bucketIndexPath, data);
        }
    });

    return async () => {};
}
