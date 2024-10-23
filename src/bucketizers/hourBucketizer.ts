import { AddRelation, Bucketizer, HourFragmentation } from "./index";
import { getLoggerFor } from "../utils/logUtil";
import { Bucket, RdfThing, Record } from "../utils";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";

const { literal, namedNode } = DataFactory;

export default class HourBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private readonly unorderedRelations: boolean;

    private hour: Date;
    private root: boolean = true;

    constructor(config: HourFragmentation, save?: string) {
        this.path = config.path.mapAll((x) => ({
            value: x.id.value,
            literal: x.id,
        }));
        this.pathQuads = config.pathQuads;
        this.unorderedRelations = config.unorderedRelations ?? false;

        if (save) {
            const parsed = JSON.parse(save);
            this.hour = new Date(parsed.hour);
            this.root = parsed.root;
        }
    }

    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
        addRelation: AddRelation,
    ): Bucket[] {
        const values = this.path
            .execute(record.data)
            .filter(
                (x, i, arr) => arr.findIndex((y) => x.value === y.value) == i,
            );

        const out: Bucket[] = [];

        for (const value of values) {
            if (value.literal) {
                // The record has a timestamp value.
                const timestamp = value.literal.value;

                const recordDate = new Date(timestamp);

                if (!this.hour) {
                    // Create the first (root) bucket.
                    this.root = true;
                    this.hour = new Date(
                        recordDate.getFullYear(),
                        recordDate.getMonth(),
                        recordDate.getDate(),
                        recordDate.getHours(),
                    );
                    out.push(getBucket(this.hour.toISOString(), this.root));

                    this.logger.debug(
                        `Created root hour bucket ${this.hour.toISOString()}`,
                    );
                } else if (recordDate.getHours() !== this.hour.getHours()) {
                    // Create a new bucket.
                    const newHour = new Date(
                        recordDate.getFullYear(),
                        recordDate.getMonth(),
                        recordDate.getDate(),
                        recordDate.getHours(),
                    );
                    const newBucket = getBucket(newHour.toISOString(), false);

                    // Add a relation from and to the previous bucket.
                    const oldBucket = getBucket(
                        this.hour.toISOString(),
                        this.root,
                    );
                    this.root = false;
                    if (this.unorderedRelations) {
                        addRelation(oldBucket, newBucket, TREE.terms.Relation);
                        addRelation(newBucket, oldBucket, TREE.terms.Relation);
                    } else {
                        addRelation(
                            oldBucket,
                            newBucket,
                            TREE.terms.GreaterThanOrEqualToRelation,
                            literal(
                                this.hour.toISOString(),
                                namedNode(XSD.dateTime),
                            ),
                            this.pathQuads,
                        );
                        addRelation(
                            newBucket,
                            oldBucket,
                            TREE.terms.LessThanRelation,
                            literal(
                                this.hour.toISOString(),
                                namedNode(XSD.dateTime),
                            ),
                            this.pathQuads,
                        );
                    }

                    // Mark the old bucket as immutable.
                    oldBucket.immutable = true;

                    out.push(newBucket);
                    this.hour = newHour;

                    this.logger.debug(
                        `Created new hour bucket ${this.hour.toISOString()}`,
                    );
                } else {
                    // The record belongs to the current bucket.
                    out.push(getBucket(this.hour.toISOString(), this.root));
                }
            } else {
                // The record does not have a timestamp value.
                this.logger.warn(
                    `Received records without timestamp values. Ignoring record '${record.data.id.value}'.`,
                );
            }
        }

        return out;
    }

    save() {
        return JSON.stringify({
            hour: this.hour,
            root: this.root,
        });
    }
}
