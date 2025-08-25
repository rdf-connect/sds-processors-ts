import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { Quad, Quad_Predicate, Term } from "@rdfjs/types";
import { readFileSync, writeFileSync } from "fs";
import { DataFactory } from "rdf-data-factory";
import { NamedNode, Parser, Quad_Object, Writer as N3Writer } from "n3";
import { Extractor } from "./utils";
import { CBDShapeExtractor } from "extract-cbd-shape";

// @ts-expect-error no declaration file found
import { canonize } from "rdf-canonize";
import { SDS, XSD } from "@treecg/types";
import { handleExit } from "./exitHandler";
import { Logger } from "winston";

const df = new DataFactory();

type StateDict = { [identify: string]: Quad[] };
type StateCache = { [identify: string]: string };

function getPrevState(path: string): StateCache {
    try {
        return JSON.parse(readFileSync(path).toString());
    } catch (ex: unknown) {
        return {};
    }
}

function createCache(quads: Quad[], keys: string[]): string {
    let out = "";
    for (const key of keys) {
        const v = quads.find((v) => v.predicate.value === key);
        if (v) {
            out += v.object.value;
        } else {
            out += "undefined";
        }
        out += "+";
    }

    return out.slice(0, -1);
}

const IS_VERSION_OF = df.namedNode("http://purl.org/dc/terms/isVersionOf");
const MODIFIED = df.namedNode("http://purl.org/dc/terms/modified");
const TIMESTAMP = df.namedNode("http://www.w3.org/2001/XMLSchema#dateTime");

class Transformer {
    private readonly modifiedPath: Term;
    private readonly isVersionOfPath: Term;

    private cache: StateCache;

    constructor(modifiedPath: Term, isVersionOfPath: Term, cache?: StateCache) {
        this.modifiedPath = modifiedPath;
        this.isVersionOfPath = isVersionOfPath;
        if (cache) {
            this.cache = cache;
        } else {
            this.cache = {};
        }
    }

    save(): string {
        return JSON.stringify(this.cache);
    }

    simpleTransform(input: string): string {
        const quads = new Parser().parse(input);

        const newState: StateDict = {};
        const ids = new Set<string>();
        const keys = new Set<string>();

        const out: Quad[] = [];

        for (const quad of quads) {
            if (!newState[quad.subject.value]) {
                newState[quad.subject.value] = [];
            }

            newState[quad.subject.value].push(quad);

            ids.add(quad.subject.value);
            keys.add(quad.predicate.value);
        }

        const key_sorted = [...keys];
        key_sorted.sort();

        for (const v of Object.values(newState)) {
            const subject = v[0].subject;

            const date = new Date().toISOString();
            const sub = df.namedNode(v[0].subject.value + "#" + date);

            out.push(
                df.quad(
                    sub,
                    <Quad_Predicate>this.modifiedPath,
                    df.literal(date, TIMESTAMP),
                ), // VersionOf
                df.quad(sub, <Quad_Predicate>this.isVersionOfPath, subject), // Timestamp
            );

            for (const q of v) {
                out.push(df.quad(sub, q.predicate, q.object));
            }
        }

        const st = new N3Writer().quadsToString(out);
        return st;
    }

    transformCheckState(input: string): string {
        const quads = new Parser().parse(input);

        const newState: StateDict = {};
        const newCache: StateCache = {};
        const ids = new Set<string>();
        const keys = new Set<string>();

        const out: Quad[] = [];

        for (const quad of quads) {
            if (!newState[quad.subject.value]) {
                newState[quad.subject.value] = [];
            }

            newState[quad.subject.value].push(quad);

            ids.add(quad.subject.value);
            keys.add(quad.predicate.value);
        }

        const key_sorted = [...keys];
        key_sorted.sort();

        for (const [k, v] of Object.entries(newState)) {
            newCache[k] = createCache(v, key_sorted);

            if (newCache[k] === this.cache[k]) {
                continue;
            }

            const subject = v[0].subject;

            const date = new Date().toISOString();
            const sub = df.namedNode(v[0].subject.value + "#" + date);

            out.push(
                df.quad(
                    sub,
                    <Quad_Predicate>this.modifiedPath,
                    df.literal(date, TIMESTAMP),
                ), // VersionOf
                df.quad(sub, <Quad_Predicate>this.isVersionOfPath, subject), // Timestamp
            );

            for (const q of v) {
                out.push(df.quad(sub, q.predicate, q.object));
            }
        }

        this.cache = newCache;

        const st = new N3Writer().quadsToString(out);
        return st;
    }
}

type Args = {
    reader: Reader;
    writer: Writer;
    statePath?: string;
    check_properties: boolean;
    modifiedPath?: Term;
    isVersionOfPath?: Term;
};
export class Ldesify extends Processor<Args> {
    transformer: Transformer;
    constructor(args: Args, logger: Logger) {
        super(Object.assign({ check_properties: true }, args), logger);
    }
    async init(this: Args & this): Promise<void> {
        let cache = {};
        if (this.statePath) cache = getPrevState(this.statePath);
        this.transformer = new Transformer(
            this.modifiedPath || MODIFIED,
            this.isVersionOfPath || IS_VERSION_OF,
            cache,
        );

        handleExit(() => {
            if (this.statePath) {
                writeFileSync(this.statePath, this.transformer.save(), {
                    encoding: "utf8",
                });
            }
        });
    }
    async transform(this: Args & this): Promise<void> {
        for await (const x of this.reader.strings()) {
            const st = this.check_properties
                ? this.transformer.transformCheckState(x)
                : this.transformer.simpleTransform(x);
            await this.writer.string(st);
        }

        if (this.statePath) {
            writeFileSync(this.statePath, this.transformer.save(), {
                encoding: "utf8",
            });
        }
        await this.writer.close();
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }
}
type SDSArgs = {
    reader: Reader;
    writer: Writer;
    statePath: string | undefined;
    sourceStream: Term | undefined;
    targetStream: Term;
    modifiedPathM?: Term; // This are the things that are added to the new entities, not necessarily related to the real objects
    isVersionOfPathM?: Term;
};
export class LdesifySDS extends Processor<SDSArgs> {
    cache: { [key: string]: string } = {};

    async init(this: SDSArgs & this): Promise<void> {
        if (this.statePath) this.cache = getPrevState(this.statePath);

        handleExit(() => {
            if (this.statePath) {
                writeFileSync(this.statePath, JSON.stringify(this.cache), {
                    encoding: "utf8",
                });
            }
        });
    }
    async transform(this: SDSArgs & this): Promise<void> {
        const modifiedPath: Term = this.modifiedPathM || MODIFIED;
        const versionPath: Term = this.isVersionOfPathM || IS_VERSION_OF;
        const extractor = new Extractor(
            new CBDShapeExtractor(),
            this.sourceStream,
        );
        for await (const x of this.reader.strings()) {
            const quads = new Parser().parse(x);

            const records = await extractor.parse_records(quads);

            for (const rec of records) {
                if (rec.data.quads.length === 0) continue;

                const hash = await canonize(rec.data.quads, {
                    algorithm: "RDFC-1.0",
                });
                if (this.cache[rec.data.id.value] === hash) continue;

                this.cache[rec.data.id.value] = hash;

                const date = new Date();
                const addHashtag = rec.data.id.value.includes("#") ? "-" : "#";
                const id = new NamedNode(
                    rec.data.id.value + addHashtag + date.getTime(),
                );

                const quads = [
                    df.quad(
                        id,
                        <Quad_Predicate>modifiedPath,
                        df.literal(date.toISOString(), XSD.terms.dateTime),
                    ),

                    df.quad(
                        id,
                        <Quad_Predicate>versionPath,
                        <Quad_Object>rec.data.id,
                    ),

                    ...rec.data.quads.map((q) =>
                        df.quad(q.subject, q.predicate, q.object, id),
                    ),
                ];

                quads.push(
                    df.quad(
                        id,
                        SDS.terms.payload,
                        id,
                        SDS.terms.custom("DataDescription"),
                    ),
                    df.quad(
                        id,
                        SDS.terms.stream,
                        <Quad_Object>this.targetStream,
                        SDS.terms.custom("DataDescription"),
                    ),
                );

                await this.writer.string(new N3Writer().quadsToString(quads));
            }
        }

        if (this.statePath) {
            writeFileSync(this.statePath, JSON.stringify(this.cache), {
                encoding: "utf8",
            });
        }
        await this.writer.close();
    }
    async produce(this: SDSArgs & this): Promise<void> {
        // nothing
    }
}
