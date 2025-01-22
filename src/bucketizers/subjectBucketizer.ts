import { AddRelation, Bucketizer, SubjectFragmentation } from "./index";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { Bucket, RdfThing, Record } from "../utils";
import { TREE } from "@treecg/types";
import { getLoggerFor } from "../utils/logUtil";

export default class SubjectBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly pathQuads: RdfThing;
    private readonly namePath?: BasicLensM<Cont, Cont>;
    private readonly defaultName?: string;

    private seen: Set<string> = new Set();

    constructor(config: SubjectFragmentation, save?: string) {
        this.path = config.path.mapAll((x) => ({
            value: x.id.value,
            literal: x.id,
        }));
        this.pathQuads = config.pathQuads;
        this.namePath = config.namePath;
        this.defaultName = config.defaultName;

        if (save) {
            this.seen = new Set(JSON.parse(save));
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

        const root = getBucket("", true);

        if (values.length === 0 && this.defaultName) {
            values.push({ value: this.defaultName });
        }

        if (values.length === 0) {
            this.logger.error(
                "Didn't find bucket, and default name is not set, sadness :(",
            );
        }

        for (const value of values) {
            const name = encodeURIComponent(
                value.literal
                    ? this.namePath?.execute({
                          id: value.literal,
                          quads: record.data.quads,
                      })[0]?.id.value || value.value
                    : value.value,
            );

            const bucket = getBucket(name);

            if (!this.seen.has(bucket.id.value)) {
                this.seen.add(bucket.id.value);

                addRelation(
                    root,
                    bucket,
                    TREE.terms.EqualToRelation,
                    value.literal,
                    value.literal ? this.pathQuads : undefined,
                );
            }

            out.push(bucket);
        }

        return out;
    }

    save() {
        return JSON.stringify([...this.seen.values()]);
    }
}
