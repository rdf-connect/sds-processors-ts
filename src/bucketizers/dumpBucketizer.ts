import { Bucketizer, DumpFragmentation } from "./index";
import { getLoggerFor } from "../utils/logUtil";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import {
    Bucket,
    findAndValidateRecordTimestamp,
    RdfThing,
    Record,
} from "../utils";

export default class DumpBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    protected readonly path: BasicLensM<
        Cont,
        { value: string; literal?: Term }
    >;
    protected readonly pathQuads: RdfThing;
    protected lastMemberTimestamp: number = 0;

    constructor(config: DumpFragmentation, save?: string) {
        if (config.path && config.pathQuads) {
            // Timestamp path is set, so we have an ordered dump bucketizer.
            this.path = config.path.mapAll((x) => ({
                value: x.id.value,
                literal: x.id,
            }));
            this.pathQuads = config.pathQuads;
        }

        if (save) {
            const parsed = JSON.parse(save);
            this.lastMemberTimestamp = parsed.lastMemberTimestamp;
        }
    }

    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
    ): Bucket[] {
        const recordTimestamp: Date | undefined | null =
            findAndValidateRecordTimestamp(
                record,
                this.path,
                this.lastMemberTimestamp,
                this.logger,
            );
        if (recordTimestamp === null) {
            return [];
        }
        if (recordTimestamp) {
            this.lastMemberTimestamp = recordTimestamp.getTime();
        }

        const bucket = getBucket("", true);
        return [bucket];
    }

    save() {
        return JSON.stringify({
            lastMemberTimestamp: this.lastMemberTimestamp,
        });
    }
}
