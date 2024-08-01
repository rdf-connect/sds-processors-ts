import { AddRelation, Bucketizer, TimebasedFragmentation } from "./index";
import { Bucket, Member, RdfThing, Record } from "../utils";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";
import literal = DataFactory.literal;
import namedNode = DataFactory.namedNode;
import { getLoggerFor } from "../utils/logUtil";

export default class TimebasedBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private readonly maxSize: number = 100;
    private readonly k: number = 4;
    private readonly minBucketSpan: number = 300000;

    private mutableLeafBucketKeys: Array<string> = [];
    private members: Array<Member> = [];

    constructor(config: TimebasedFragmentation, save?: string) {
        this.path = config.path.mapAll((x) => ({
            value: x.id.value,
            literal: x.id,
        }));
        this.pathQuads = config.pathQuads;
        this.maxSize = config.maxSize;
        this.k = config.k;
        this.minBucketSpan = config.minBucketSpan * 1000;

        if (save) {
            const parsed = JSON.parse(save);
            this.mutableLeafBucketKeys = parsed.mutableLeafBucketKeys;
            this.members = parsed.members;
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

                // Find the bucket where the record belongs.
                let candidateBucket: Bucket | undefined = undefined;
                let bucketKey = this.mutableLeafBucketKeys[0];
                while (!candidateBucket) {
                    if (!bucketKey) {
                        // No more leaf buckets to check. We need to generate a new year bucket.
                        const root = getBucket("root", true);
                        const newBucketTimestamp = new Date(timestamp);
                        newBucketTimestamp.setUTCMonth(0);
                        newBucketTimestamp.setUTCDate(1);
                        newBucketTimestamp.setUTCHours(0, 0, 0, 0);
                        // Support leap years.
                        const yearTimespan =
                            new Date(
                                newBucketTimestamp.getUTCFullYear(),
                                1,
                                29,
                            ).getDate() === 29
                                ? 31622400000
                                : 31536000000;

                        this.logger.info(
                            `Creating new year bucket with timestamp ${newBucketTimestamp.toISOString()} and timespan ${yearTimespan}.`,
                        );

                        const yearBucket = getBucket(
                            `${newBucketTimestamp.toISOString()}_${yearTimespan}_0`,
                            false,
                        );

                        // Add the new year bucket to the root bucket
                        this.addTimestampRelations(
                            root,
                            yearBucket,
                            newBucketTimestamp,
                            yearTimespan,
                            addRelation,
                        );

                        // Add the new bucket to the list of leaf buckets.
                        this.mutableLeafBucketKeys.push(yearBucket.id.value);
                        this.members = [];
                        bucketKey = yearBucket.id.value;
                    }

                    const bucket = getBucket(bucketKey);

                    // Check if the record belongs to the current bucket.
                    const bucketNames = bucket.id.value.split("/");
                    const bucketProperties = decodeURIComponent(
                        bucketNames[bucketNames.length - 1],
                    ).split("_");
                    const bucketTimestamp = new Date(bucketProperties[0]);
                    const bucketSpan = parseInt(bucketProperties[1]);
                    const recordTimestamp = new Date(timestamp);
                    if (recordTimestamp.getTime() < bucketTimestamp.getTime()) {
                        // This should not happen! The record timestamp is before the bucket timestamp of the smallest leaf bucket.
                        this.logger.error(
                            `Record timestamp is before the smallest mutable bucket timestamp. Are your records out of order? Ignoring record '${record.data.id.value}'.`,
                        );
                        return [];
                    } else if (
                        recordTimestamp.getTime() >=
                        bucketTimestamp.getTime() + bucketSpan
                    ) {
                        // The record timestamp is after the current bucket span. We need to check the next leaf bucket.
                        // Make this bucket immutable as a record with a later timestamp arrived.
                        bucket.immutable = true;

                        this.logger.debug(
                            `Record timestamp is after the current bucket span. Making bucket '${bucket.id.value}' immutable.`,
                        );

                        // Remove the current bucket from the list of leaf buckets.
                        this.mutableLeafBucketKeys.shift();
                        this.members = [];
                        bucketKey = this.mutableLeafBucketKeys[0];

                        // Check if we should also make the parent immutable.
                        this.makeParentImmutableIfNoMutableChildren(
                            bucket,
                            getBucket,
                        );
                    } else {
                        // The record belongs to the current bucket.
                        candidateBucket = bucket;
                    }
                }

                // Is there still space in the bucket?
                while (this.members.length >= this.maxSize) {
                    // The bucket is full. We need to split it.
                    const bucketNames = candidateBucket.id.value.split("/");
                    const bucketProperties = decodeURIComponent(
                        bucketNames[bucketNames.length - 1],
                    ).split("_");

                    // Check if we should split or make a new page.
                    if (
                        parseInt(bucketProperties[1]) / this.k <
                        this.minBucketSpan
                    ) {
                        this.logger.debug("We need to make a new page");
                        // We need to make a new page.
                        const newBucket = getBucket(
                            `${bucketProperties[0]}_${bucketProperties[1]}_${
                                parseInt(bucketProperties[2]) + 1
                            }`,
                        );

                        // Make the old bucket as immutable and add the relation to the new bucket.
                        candidateBucket.immutable = true;
                        addRelation(
                            candidateBucket,
                            newBucket,
                            TREE.terms.Relation,
                        );

                        // Update the members for the new bucket.
                        this.members = [];

                        // Add the new bucket to the list of leaf buckets at the position of the old bucket, which was at index 0.
                        this.mutableLeafBucketKeys[0] = newBucket.id.value;

                        // The record belongs in this newBucket, so make newBucket the candidateBucket.
                        candidateBucket = newBucket;
                    } else {
                        this.logger.debug("We need to split the bucket");
                        // We need to split the bucket.
                        const newBucketSpan = Math.round(
                            parseInt(bucketProperties[1]) / this.k,
                        );
                        const newMutableLeafBuckets = [];
                        const parentBucket = candidateBucket;
                        parentBucket.empty = true;

                        const recordTime = new Date(timestamp).getTime();
                        for (let i = 0; i < this.k; i++) {
                            const newTimestamp = new Date(
                                new Date(bucketProperties[0]).getTime() +
                                    i * newBucketSpan,
                            );
                            const newBucket = getBucket(
                                `${newTimestamp.toISOString()}_${newBucketSpan}_0`,
                            );

                            // Add the members that belong to the new bucket.
                            this.members
                                .filter(
                                    (m) =>
                                        m.timestamp >= newTimestamp.getTime() &&
                                        m.timestamp <
                                            newTimestamp.getTime() +
                                                newBucketSpan,
                                )
                                .forEach((m) => newBucket.addMember(m.id));

                            // Only add it to the mutable leaf buckets if the record is in this or a later bucket.
                            if (
                                recordTime <
                                newTimestamp.getTime() + newBucketSpan
                            ) {
                                newMutableLeafBuckets.push(newBucket.id.value);

                                // If the record is in this bucket, update its members and add the record to it.
                                if (recordTime >= newTimestamp.getTime()) {
                                    // Neglect the records that are now part of an earlier bucket i in the k-split.
                                    this.members = this.members.filter(
                                        (m) =>
                                            m.timestamp >=
                                                newTimestamp.getTime() &&
                                            m.timestamp <
                                                newTimestamp.getTime() +
                                                    newBucketSpan,
                                    );

                                    // The record should have a later timestamp than the last record in the bucket.
                                    if (this.members.length > 0) {
                                        const lastTimestamp =
                                            this.members[
                                                this.members.length - 1
                                            ].timestamp;
                                        if (recordTime < lastTimestamp) {
                                            // The record is out of order. This should not happen.
                                            this.logger.error(
                                                `Record timestamp is before the last record timestamp in the new split bucket. Are your records out of order? Ignoring record '${record.data.id.value}'.`,
                                            );
                                            return [];
                                        }
                                    }

                                    // The record belongs in this newBucket, so make newBucket the candidateBucket.
                                    candidateBucket = newBucket;
                                }
                            } else {
                                // Otherwise, make the bucket immutable.
                                newBucket.immutable = true;
                            }

                            // Add the relations for the new bucket.
                            this.addTimestampRelations(
                                parentBucket,
                                newBucket,
                                newTimestamp,
                                newBucketSpan,
                                addRelation,
                            );
                        }

                        // Update the mutable leaf buckets
                        this.mutableLeafBucketKeys = [
                            ...newMutableLeafBuckets,
                            ...this.mutableLeafBucketKeys.slice(1),
                        ];
                    }
                }

                // There is still space in the bucket. Check if the record is not out of order.
                if (this.members.length > 0) {
                    const lastTimestamp =
                        this.members[this.members.length - 1].timestamp;
                    if (new Date(timestamp).getTime() < lastTimestamp) {
                        // The record is out of order. This should not happen.
                        this.logger.error(
                            `Record timestamp is before the last record timestamp. Are your records out of order? Ignoring record '${record.data.id.value}'.`,
                        );
                        return [];
                    }
                }

                // Add the record to the bucket.
                this.members.push({
                    id: JSON.parse(JSON.stringify(record.data.id.value)),
                    timestamp: new Date(timestamp).getTime(),
                });

                // The record belongs in this candidateBucket, so return it.
                out.push(candidateBucket);
            } else {
                // The record does not have a timestamp value.
                this.logger.warn(
                    `Received records without timestamp values. Ignoring record '${record.data.id.value}'.`,
                );
            }
        }

        return out;
    }

    save(): string {
        return JSON.stringify({
            mutableLeafBucketKeys: this.mutableLeafBucketKeys,
            members: this.members,
        });
    }

    private addTimestampRelations(
        rootBucket: Bucket,
        childBucket: Bucket,
        startTimestamp: Date,
        timespan: number,
        addRelation: AddRelation,
    ) {
        addRelation(
            rootBucket,
            childBucket,
            TREE.terms.GreaterThanOrEqualToRelation,
            literal(startTimestamp.toISOString(), namedNode(XSD.dateTime)),
            this.pathQuads,
        );

        addRelation(
            rootBucket,
            childBucket,
            TREE.terms.LessThanRelation,
            literal(
                new Date(startTimestamp.getTime() + timespan).toISOString(),
                namedNode(XSD.dateTime),
            ),
            this.pathQuads,
        );
    }

    private makeParentImmutableIfNoMutableChildren(
        bucket: Bucket,
        getBucket: (key: string, root?: boolean) => Bucket,
    ) {
        const parent = bucket.parent;
        if (parent && !parent.root) {
            // Check if all its children are immutable.
            const children = parent.links.map((link) => {
                return getBucket(link.target.value);
            });
            const mutableChild = children.find((child) => !child.immutable);
            if (mutableChild === undefined) {
                parent.immutable = true;
                this.logger.debug(
                    `Making parent bucket '${parent.id.value}' immutable.`,
                );
                this.makeParentImmutableIfNoMutableChildren(parent, getBucket);
            }
        }
    }
}
