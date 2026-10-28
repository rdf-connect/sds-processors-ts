import { Quad, Quad_Object, Term } from "@rdfjs/types";
import { Processor, Reader } from "@rdfc/js-runner";
import { maybeParse } from "./utils";
import { dedupQuads, Pipeline } from "./pipeline";
import { Extractor } from "./utils/description";
import path from "node:path";
import { DataFactory } from "rdf-data-factory";
import { DC, LDES, RDF, SDS, TREE, XSD } from "@treecg/types";
import { Parser, Writer } from "n3";
import * as fs from "node:fs";
import { toPath } from "@rdfc/utils-js";

const df = new DataFactory({ blankNodePrefix: "df_" + new Date().getTime() });

// for an i, add /0/1/2/3/.../i to the base URI
const INTERNAL_TEMP_BASE_HOST = "http://ldes-disk-writer.internal/";
const INTERNAL_TEMP_BASE_URI =
    INTERNAL_TEMP_BASE_HOST +
    Array.from({ length: 20 }, (_, i) => i)
        .map((i) => i.toString())
        .join("/") +
    "/";

type MapEntry = {
    stream: Term;
    name: string;
};

type Args = {
    data: Reader;
    directory: string;
    nameMap: MapEntry[];
    // The whole pipeline description (rdfl:Context), to describe the streams that reach this writer.
    pipeline: Quad[];
};

export class LdesDiskWriter extends Processor<Args> {
    extractor = new Extractor();
    pipelineGraph: Pipeline;
    // The streams published as views.
    streams: Term[] = [];

    async init(this: Args & this): Promise<void> {
        this.directory = toPath(this.directory);
        this.pipelineGraph = new Pipeline(this.pipeline || []);
        this.streams = this.pipelineGraph.streamsOn(this.data.uri);
    }

    async transform(this: Args & this): Promise<void> {
        try {
            await this.writeRoot();
            await this.handleData();
        } catch (ex: unknown) {
            console.error(ex);
            throw ex;
        }
    }

    async produce(this: Args & this): Promise<void> {
        // nothing
    }

    pathName(
        this: Args & this,
        stream: Term | string,
        alreadyEncoded = false,
    ): string {
        const value = typeof stream === "string" ? stream : stream.value;

        const eq =
            typeof stream === "string"
                ? (q: MapEntry) => q.stream.value === stream
                : (q: MapEntry) => q.stream.equals(stream);

        const entry = this.nameMap.find(eq);
        if (entry === undefined) {
            return encodePathValue(value, alreadyEncoded);
        } else {
            return entry.name;
        }
    }

    /**
     * Writes the root of the LDES: one view for every stream, and how each
     * stream was derived, as far as the pipeline description tells.
     */
    async writeRoot(this: Args & this) {
        const root = df.namedNode("index.trig");
        const metadataQuads = dedupQuads(
            this.streams.flatMap((stream) =>
                this.pipelineGraph.describe(stream),
            ),
        );
        metadataQuads.push(
            df.quad(root, RDF.terms.type, LDES.terms.EventStream),
        );

        // The LDES is the dataset of its streams, and its members have their shape.
        const objectOfStreams = (predicate: Term) =>
            metadataQuads.find(
                (q) =>
                    this.streams.some((s) => s.equals(q.subject)) &&
                    q.predicate.equals(predicate),
            )?.object;
        const datasetId = objectOfStreams(SDS.terms.dataset);
        if (datasetId) {
            metadataQuads
                .filter((q) => q.subject.equals(datasetId))
                .forEach((q) =>
                    metadataQuads.push(df.quad(root, q.predicate, q.object)),
                );
        }
        const carries = objectOfStreams(SDS.terms.carries);
        const shapeId = carries
            ? metadataQuads.find(
                  (q) =>
                      q.subject.equals(carries) &&
                      q.predicate.equals(SDS.terms.shape),
              )?.object
            : undefined;
        if (
            shapeId &&
            !metadataQuads.some(
                (q) =>
                    q.subject.equals(root) &&
                    q.predicate.equals(TREE.terms.shape),
            )
        ) {
            metadataQuads.push(df.quad(root, TREE.terms.shape, shapeId));
        }

        for (const stream of this.streams) {
            const viewId = df.namedNode(
                path.join(this.pathName(stream), "index.trig"),
            );
            const viewDescriptionId = df.namedNode(
                `${viewId.value}#description`,
            );
            metadataQuads.push(
                df.quad(root, TREE.terms.view, viewId),
                df.quad(viewId, RDF.terms.type, TREE.terms.Node),
                df.quad(
                    viewId,
                    TREE.terms.custom("viewDescription"),
                    viewDescriptionId,
                ),
                df.quad(
                    viewDescriptionId,
                    RDF.terms.type,
                    TREE.terms.custom("ViewDescription"),
                ),
                df.quad(
                    viewDescriptionId,
                    df.namedNode("http://www.w3.org/ns/dcat#endpointURL"),
                    viewId,
                ),
                df.quad(
                    viewDescriptionId,
                    df.namedNode("http://www.w3.org/ns/dcat#servesDataset"),
                    root,
                ),
                df.quad(
                    viewDescriptionId,
                    LDES.terms.custom("managedBy"),
                    <Quad_Object>stream,
                ),
            );

            // Create the directory and index file for the view
            const viewPath = path.join(this.directory, this.pathName(stream));
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

        await fs.promises.mkdir(this.directory, { recursive: true });
        await fs.promises.writeFile(
            path.join(this.directory, "index.trig"),
            await quadsToString(metadataQuads),
        );
        this.logger.debug(
            `[metadata] LDES entry point written with ${this.streams.length} views and ${metadataQuads.length} quads`,
        );
    }

    /** A stream that the pipeline description did not show reaching this writer. */
    async addStream(this: Args & this, stream: string) {
        if (this.streams.some((s) => s.value === stream)) return;
        this.logger.info(`[data] Adding a view for stream <${stream}>`);
        this.streams.push(df.namedNode(stream));
        await this.writeRoot();
    }

    async handleData(this: Args & this) {
        for await (const input of this.data.strings()) {
            const data = maybeParse(input);
            const extract = this.extractor.extract_quads(data);

            this.logger.debug(`[data] Got input with ${data.length} quads`);

            // A member is written with its IRI: a blank node payload has none.
            const records = extract.getRecords().filter((record) => {
                if (record.payload.termType === "NamedNode") return true;
                this.logger.warn(
                    `[data] Skipping a record of stream <${record.stream.value}> with a blank node payload`,
                );
                return false;
            });

            for (const stream of [
                ...extract.getBuckets().map((b) => b.stream.value),
                ...records.map((r) => r.stream.value),
            ]) {
                await this.addStream(stream);
            }

            const members: Map<string, Quad[]> = new Map<string, Quad[]>(); // member id -> quads

            // For each bucket, make sure it's directory and index file containing the metadata exists
            // And remove all members if the bucket needs to be emptied
            for (const bucket of extract.getBuckets()) {
                const bucketPath = path.join(
                    this.directory,
                    this.pathName(bucket.stream),
                    encodePathValue(bucket.id.value, true),
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
                                    this.pathName(bucket.stream),
                                    encodePathValue(bucket.id.value, true),
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
                    this.logger.debug(
                        `[data] Emptying bucket ${bucket.id.value}`,
                    );
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
                    const data = await quadsToString([
                        ...metadata,
                        ...relations,
                    ]);
                    await fs.promises.writeFile(bucketIndexPath, data);
                }

                if (bucket.root) {
                    // Check if a relation to the viewId from the bucketId already exists
                    const viewIndexPath = path.join(
                        this.directory,
                        this.pathName(bucket.stream),
                        "index.trig",
                    );
                    let content = "";
                    try {
                        content = await fs.promises.readFile(viewIndexPath, {
                            encoding: "utf8",
                        });
                    } catch (ex) {
                        // ignore
                    }
                    const existingQuads = new Parser({
                        baseIRI: INTERNAL_TEMP_BASE_URI,
                    }).parse(content);

                    const absoluteRelativeBucketId = df.namedNode(
                        new URL(
                            path.join(
                                encodePathValue(bucket.id.value, true),
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
                            df.quad(
                                bn,
                                TREE.terms.node,
                                absoluteRelativeBucketId,
                            ),
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
                            q.predicate.equals(
                                LDES.terms.custom("immutable"),
                            ) &&
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
            for (const record of records) {
                for (const bucket of record.buckets) {
                    // Append the member contents to the file corresponding to the bucket
                    const bucketIndexPath = path.join(
                        this.directory,
                        this.pathName(record.stream),
                        encodePathValue(bucket.value, true),
                        "index.trig",
                    );

                    const relativeLdesId = df.namedNode(
                        path.join(
                            path.relative(
                                path.join(
                                    this.pathName(record.stream),
                                    encodePathValue(bucket.value, true),
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
                            df.namedNode(
                                encodePathValue(record.payload.value, true),
                            ),
                        ),
                    ];
                    if (!record.dataless) {
                        quads.push(...extract.getData());
                    } else {
                        quads.push(
                            ...(members.get(
                                new URL(
                                    record.payload.value,
                                    INTERNAL_TEMP_BASE_URI,
                                ).href,
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
                    this.directory,
                    this.pathName(relation.stream),
                    encodePathValue(relation.origin.value, true),
                    "index.trig",
                );

                const content = await fs.promises.readFile(bucketIndexPath);
                const quads = new Parser({
                    baseIRI: INTERNAL_TEMP_BASE_URI,
                }).parse(content.toString());
                // Find blankNode subject of the relation and remove all related quads
                const relationTargetIndexPath = path.join(
                    encodePathValue(
                        path.relative(
                            relation.origin.value,
                            relation.bucket.value,
                        ),
                        true,
                    ),
                    "index.trig",
                );
                const blankNode = quads.find(
                    (bnQ) =>
                        bnQ.predicate.equals(RDF.terms.type) &&
                        bnQ.object.value === relation.type.value &&
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
                    this.logger.error(
                        `Could not find blankNode to remove relation ${relation.type.value} from ${relation.origin.value} to ${relation.bucket.value}`,
                    );
                    continue;
                }
                const updatedQuads = quads.filter(
                    (q) =>
                        !q.subject.equals(blankNode) &&
                        !q.object.equals(blankNode),
                );
                const data = await quadsToString(updatedQuads);

                await fs.promises.writeFile(bucketIndexPath, data);
            }

            for (const relation of extract.getRelations()) {
                // Append the relation to the file corresponding to the bucket
                const bucketIndexPath = path.join(
                    this.directory,
                    this.pathName(relation.stream),
                    encodePathValue(relation.origin.value, true),
                    "index.trig",
                );

                const bn = df.blankNode();
                const quads = [
                    df.quad(
                        df.namedNode("index.trig"),
                        TREE.terms.relation,
                        bn,
                    ),
                    df.quad(
                        bn,
                        RDF.terms.type,
                        df.namedNode(relation.type.value),
                    ),
                    df.quad(
                        bn,
                        TREE.terms.node,
                        df.namedNode(
                            path.join(
                                encodePathValue(
                                    path.relative(
                                        relation.origin.value,
                                        relation.bucket.value,
                                    ),
                                    true,
                                ),
                                "index.trig",
                            ),
                        ),
                    ),
                ];
                if (relation.path) {
                    quads.push(
                        df.quad(
                            bn,
                            TREE.terms.path,
                            <Quad_Object>relation.path.id,
                        ),
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
        }
    }
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
