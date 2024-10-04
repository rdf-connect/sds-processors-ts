import { BasicLensM, Cont } from "rdf-lens";
import { AddRelation, Bucketizer } from ".";
import { Bucket, Record } from "../utils";
import { Term } from "@rdfjs/types";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";

const { literal, namedNode } = DataFactory;

export type Level =
    | "year"
    | "month"
    | "day-of-month"
    | "hour"
    | "minute"
    | "second"
    | "millisecond";

const months = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

const levelToValue: { [key in Level]: (date: Date) => string } = {
    year: (date) => date.getUTCFullYear() + "",
    month: (date) => months[date.getUTCMonth()],
    "day-of-month": (date) => date.getUTCDate() + "",
    hour: (date) => date.getUTCHours() + "",
    minute: (date) => date.getUTCMinutes() + "",
    second: (date) => date.getUTCSeconds() + "",
    millisecond: (date) => date.getUTCMilliseconds() + "",
};

const levelMin: { [key in Level]: (date: Date) => Date } = {
    year: (date) => {
        return new Date(Date.UTC(date.getUTCFullYear(), 0));
    },
    month: (date) => {
        return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth()));
    },
    "day-of-month": (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
            ),
        );
    },
    hour: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
            ),
        );
    },
    minute: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes(),
            ),
        );
    },
    second: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes(),
                date.getUTCSeconds(),
            ),
        );
    },
    millisecond: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes(),
                date.getUTCSeconds(),
                date.getUTCMilliseconds(),
            ),
        );
    },
};

const levelMax: { [key in Level]: (date: Date) => Date } = {
    year: (date) => {
        return new Date(Date.UTC(date.getUTCFullYear() + 1, 0));
    },
    month: (date) => {
        return new Date(
            Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1),
        );
    },
    "day-of-month": (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate() + 1,
            ),
        );
    },
    hour: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours() + 1,
            ),
        );
    },
    minute: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes() + 1,
            ),
        );
    },
    second: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes(),
                date.getUTCSeconds() + 1,
            ),
        );
    },
    millisecond: (date) => {
        return new Date(
            Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                date.getUTCHours(),
                date.getUTCMinutes(),
                date.getUTCSeconds(),
                date.getUTCMilliseconds() + 1,
            ),
        );
    },
};

const allowed: { [key in Level]: Level[] } = {
    year: [],
    month: ["year"],
    "day-of-month": ["month"],
    hour: ["day-of-month"],
    minute: ["hour"],
    second: ["minute"],
    millisecond: ["second"],
};

// pft this function is ugly
// only allow logic things:
//   month can only come after year, otherwise duplicate keys are a thing
function levelsAreValid(levels: Level[]): boolean {
    if (levels.length < 2) {
        return levels[0] == "year";
    }
    return levels.slice(1).every((v, i) => {
        const prev = levels[i];
        return allowed[v].includes(prev);
    });
}

export type TimeBucketTreeConfig = {
    path: BasicLensM<Cont, Cont>;
    pathQuads: Cont;
    maxSize: number;
    levels: Level[];
    timeBufferMs: number;
};

type State = {
    [value: string]: {
        deep: State;
        count: number;
        end: Date;
        immutable: boolean;
    };
};

function hydrate(state: State) {
    for (const key of Object.keys(state)) {
        state[key].end = new Date(state[key].end);
        hydrate(state[key].deep);
    }
}

export default class TimeBucketTreeBucketizer implements Bucketizer {
    private readonly config: TimeBucketTreeConfig;
    private readonly path: BasicLensM<Cont, { value: string; literal?: Term }>;
    private readonly state: State = {};

    constructor(config: TimeBucketTreeConfig, save?: string) {
        if (!levelsAreValid(config.levels)) {
            throw (
                "Levels are not valid, duplicate keys are a thing! " +
                JSON.stringify(config.levels)
            );
        }

        if (config["timeBufferMs"] === undefined) {
            config["timeBufferMs"] = 0;
        }
        this.config = config;

        this.path = config.path.mapAll((x) => ({
            value: x.id.value,
            literal: x.id,
        }));

        if (save) {
            this.state = JSON.parse(save);
            hydrate(this.state);
        }
    }

    bucketize(
        sdsMember: Record,
        getBucket: (key: string, root?: boolean) => Bucket,
        addRelation: AddRelation,
    ): Bucket[] {
        const values = this.path
            .execute(sdsMember.data)
            .filter(
                (x, i, arr) => arr.findIndex((y) => x.value === y.value) == i,
            );

        const out: Bucket[] = [];
        for (const value of values) {
            const date = new Date(value.value);
            const endDate = new Date(date.getTime() - this.config.timeBufferMs);
            let key = "";
            let state = this.state;
            let bucket = getBucket(key, true);
            for (const level of this.config.levels) {
                checkImmutable(state, key, endDate, getBucket);

                const levelValue = levelToValue[level](date);
                const found = goInState(
                    state,
                    levelValue,
                    date,
                    levelMax[level],
                );

                state = found.value.deep;
                key = concat_key(key, levelValue);

                const nextBucket = getBucket(key);
                if (!found.found) {
                    const minDate = levelMin[level](date).toISOString();
                    const maxDate = levelMax[level](date).toISOString();

                    addRelation(
                        bucket,
                        nextBucket,
                        TREE.terms.GreaterThanOrEqualToRelation,
                        literal(minDate, namedNode(XSD.dateTime)),
                        this.config.pathQuads,
                    );

                    addRelation(
                        bucket,
                        nextBucket,
                        TREE.terms.LessThanRelation,
                        literal(maxDate, namedNode(XSD.dateTime)),
                        this.config.pathQuads,
                    );
                }
                bucket = nextBucket;
                if (found.value.count < this.config.maxSize) {
                    found.value.count += 1;
                    break;
                }
            }

            out.push(bucket);
        }

        return out;
    }

    save(): string {
        return JSON.stringify(this.state);
    }
}

function concat_key(path: string, key: string): string {
    if (path.length === 0) {
        return key;
    } else {
        return `${path}/${key}`;
    }
}

function checkImmutable(
    state: State,
    path: string,
    end: Date,
    getBucket: (key: string, root?: boolean) => Bucket,
) {
    for (const key of Object.keys(state)) {
        const inner = state[key];
        if (!inner.immutable && inner.end < end) {
            const innerPath = concat_key(path, key);
            const bucket = getBucket(innerPath);
            inner.immutable = true;
            bucket.immutable = true;
            checkImmutable(inner.deep, innerPath, end, getBucket);
        }
    }
}

function goInState(
    state: State,
    value: string,
    date_value: Date,
    end_f: (value: Date) => Date,
): { found: boolean; value: { deep: State; count: number } } {
    const out = state[value];
    if (out) return { found: true, value: out };

    state[value] = {
        deep: {},
        count: 0,
        end: end_f(date_value),
        immutable: false,
    };
    return { found: false, value: state[value] };
}
