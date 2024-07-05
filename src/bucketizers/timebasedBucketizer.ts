import { Bucketizer, TimebasedFragmentation } from "./index";
import { Bucket, RdfThing, Record } from "../utils";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";
import literal = DataFactory.literal;
import namedNode = DataFactory.namedNode;

export default class TimebasedBucketizer implements Bucketizer {
    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private readonly maxSize: number = 100;
    private readonly k: number = 4;
    private readonly minBucketSpan: number = 300000;

    private mutableLeafBucketKeys: Array<string> = [];
    private memberTimestamps: Array<string> = [];

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
            this.memberTimestamps = parsed.memberTimestamps;
        }
    }

    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
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
                        // TODO: Add support for leap years.
                        const yearTimespan = 31536000000;
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
                        );

                        // Add the new bucket to the list of leaf buckets.
                        this.mutableLeafBucketKeys.push(yearBucket.id.value);
                        this.memberTimestamps = [];
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
                        throw new Error(
                            "This should not happen! Record timestamp is before the smallest mutable bucket timestamp. Are your records out of order?",
                        );
                    } else if (
                        recordTimestamp.getTime() >=
                        bucketTimestamp.getTime() + bucketSpan
                    ) {
                        // The record timestamp is after the current bucket span. We need to check the next leaf bucket.
                        // Make this bucket immutable as a record with a later timestamp arrived.
                        bucket.immutable = true;

                        // Remove the current bucket from the list of leaf buckets.
                        this.mutableLeafBucketKeys.shift();
                        this.memberTimestamps = [];
                        bucketKey = this.mutableLeafBucketKeys[0];

                        // If the new current bucket is defined and has a different parent as the former leaf bucket, we make the parent of the former bucket immutable as it has no more mutable children.
                        if (
                            bucket.parent &&
                            (this.mutableLeafBucketKeys.length === 0 ||
                                bucket.parent.id.value !==
                                    getBucket(bucketKey)?.parent?.id.value)
                        ) {
                            // But we don't do this with the root bucket!
                            if (!bucket.parent.root) {
                                bucket.parent.immutable = true;
                            }
                        }
                    } else {
                        // The record belongs to the current bucket.
                        candidateBucket = bucket;
                    }
                }

                // Is there still space in the bucket?
                while (this.memberTimestamps.length >= this.maxSize) {
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
                        console.log("We need to make a new page");
                        // We need to make a new page.
                        const newBucket = getBucket(
                            `${bucketProperties[0]}_${bucketProperties[1]}_${bucketProperties[2] + 1}`,
                        );

                        // Make the old bucket as immutable and add the relation to the new bucket.
                        candidateBucket.immutable = true;
                        candidateBucket.addRelation(
                            newBucket,
                            TREE.terms.Relation,
                        );

                        // Update the members for the new bucket.
                        this.memberTimestamps = [];

                        // Add the new bucket to the list of leaf buckets at the position of the old bucket, which was at index 0.
                        this.mutableLeafBucketKeys[0] = newBucket.id.value;

                        // The record belongs in this newBucket, so make newBucket the candidateBucket.
                        candidateBucket = newBucket;
                    } else {
                        console.log("We need to split the bucket");
                        // We need to split the bucket.
                        const newBucketSpan =
                            parseInt(bucketProperties[1]) / this.k;
                        const newMutableLeafBuckets = [];
                        const parentBucket = candidateBucket;

                        for (let i = 0; i < this.k; i++) {
                            const newTimestamp = new Date(
                                new Date(bucketProperties[0]).getTime() +
                                    i * newBucketSpan,
                            );
                            const newBucket = getBucket(
                                `${newTimestamp.toISOString()}_${newBucketSpan}_0`,
                            );

                            // Only add it to the mutable leaf buckets if the record is in this or a later bucket.
                            const recordTime = new Date(timestamp).getTime();
                            if (recordTime >= newTimestamp.getTime()) {
                                newMutableLeafBuckets.push(newBucket.id.value);

                                // If the record is in this bucket, update its members and add the record to it.
                                if (
                                    recordTime <
                                    new Date(
                                        newTimestamp.getTime() + newBucketSpan,
                                    ).getTime()
                                ) {
                                    // Neglect the records that are now part of an earlier bucket i in the k-split.
                                    this.memberTimestamps =
                                        this.memberTimestamps.filter(
                                            (t) =>
                                                new Date(t).getTime() >=
                                                newTimestamp.getTime(),
                                        );

                                    // The record should have a later timestamp than the last record in the bucket.
                                    if (this.memberTimestamps.length > 0) {
                                        const lastTimestamp = new Date(
                                            this.memberTimestamps[
                                                this.memberTimestamps.length - 1
                                            ],
                                        );
                                        if (
                                            recordTime < lastTimestamp.getTime()
                                        ) {
                                            // The record is out of order. This should not happen.
                                            throw new Error(
                                                "This should not happen! Record timestamp is before the last record timestamp in the new split bucket. Are your records out of order?",
                                            );
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
                if (this.memberTimestamps.length > 0) {
                    const lastTimestamp = new Date(
                        this.memberTimestamps[this.memberTimestamps.length - 1],
                    );
                    if (
                        new Date(timestamp).getTime() < lastTimestamp.getTime()
                    ) {
                        // The record is out of order. This should not happen.
                        throw new Error(
                            "This should not happen! Record timestamp is before the last record timestamp. Are your records out of order?",
                        );
                    }
                }
                // Add the record to the bucket.
                this.memberTimestamps.push(timestamp);

                // The record belongs in this candidateBucket, so return it.
                out.push(candidateBucket);
            } else {
                // The record does not have a timestamp value.
                // TODO: Handle this case: we want to ignore and warn.
                throw new Error("records without timestamp values");
            }
        }

        return out;
    }

    save(): string {
        return JSON.stringify({
            mutableLeafBucketKeys: this.mutableLeafBucketKeys,
            memberTimestamps: this.memberTimestamps,
        });
    }

    private addTimestampRelations(
        rootBucket: Bucket,
        childBucket: Bucket,
        startTimestamp: Date,
        timespan: number,
    ) {
        rootBucket.addRelation(
            childBucket,
            TREE.terms.GreaterThanOrEqualToRelation,
            literal(startTimestamp.toISOString(), namedNode(XSD.dateTime)),
            this.pathQuads,
        );
        rootBucket.addRelation(
            childBucket,
            TREE.terms.LessThanRelation,
            literal(
                new Date(startTimestamp.getTime() + timespan).toISOString(),
                namedNode(XSD.dateTime),
            ),
            this.pathQuads,
        );
    }
}
