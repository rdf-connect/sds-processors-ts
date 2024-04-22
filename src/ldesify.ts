import type { Stream, Writer } from "@ajuvercr/js-runner";
import { Term } from "@rdfjs/types";
import { readFileSync, writeFileSync } from "fs";
import * as n3 from "n3";

const { namedNode, quad, literal } = n3.DataFactory;

type StateDict = { [identify: string]: n3.Quad[] };
type StateCache = { [identify: string]: string };

function getPrevState(path: string): StateCache {
  try {
    return JSON.parse(readFileSync(path).toString());
  } catch (ex: any) {
    return {};
  }
}

function createCache(quads: n3.Quad[], keys: string[]): string {
  let out = "";
  for (let key of keys) {
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

const IS_VERSION_OF = namedNode("http://purl.org/dc/terms/isVersionOf");
const MODIFIED = namedNode("http://purl.org/dc/terms/modified");
const TIMESTAMP = namedNode("http://www.w3.org/2001/XMLSchema#dateTime");

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
    const quads = new n3.Parser().parse(input);

    const newState: StateDict = {};
    const ids = new Set<string>();
    const keys = new Set<string>();

    const out: n3.Quad[] = [];

    for (let quad of quads) {
      if (!newState[quad.subject.value]) {
        newState[quad.subject.value] = [];
      }

      newState[quad.subject.value].push(quad);

      ids.add(quad.subject.value);
      keys.add(quad.predicate.value);
    }

    const key_sorted = [...keys];
    key_sorted.sort();

    for (let v of Object.values(newState)) {
      const subject = v[0].subject;

      const date = new Date().toISOString();
      const sub = namedNode(v[0].subject.value + "#" + date);

      out.push(
        quad(
          sub,
          <n3.Quad_Predicate>this.modifiedPath,
          literal(date, TIMESTAMP),
        ), // VersionOf
        quad(sub, <n3.Quad_Predicate>this.isVersionOfPath, subject), // Timestamp
      );

      for (let q of v) {
        out.push(quad(sub, q.predicate, q.object));
      }
    }

    const st = new n3.Writer().quadsToString(out);
    return st;
  }

  transformCheckState(input: string): string {
    const quads = new n3.Parser().parse(input);

    const newState: StateDict = {};
    const newCache: StateCache = {};
    const ids = new Set<string>();
    const keys = new Set<string>();

    const out: n3.Quad[] = [];

    for (let quad of quads) {
      if (!newState[quad.subject.value]) {
        newState[quad.subject.value] = [];
      }

      newState[quad.subject.value].push(quad);

      ids.add(quad.subject.value);
      keys.add(quad.predicate.value);
    }

    const key_sorted = [...keys];
    key_sorted.sort();

    for (let [k, v] of Object.entries(newState)) {
      newCache[k] = createCache(v, key_sorted);

      if (newCache[k] === this.cache[k]) {
        continue;
      }

      const subject = v[0].subject;

      const date = new Date().toISOString();
      const sub = namedNode(v[0].subject.value + "#" + date);

      out.push(
        quad(
          sub,
          <n3.Quad_Predicate>this.modifiedPath,
          literal(date, TIMESTAMP),
        ), // VersionOf
        quad(sub, <n3.Quad_Predicate>this.isVersionOfPath, subject), // Timestamp
      );

      for (let q of v) {
        out.push(quad(sub, q.predicate, q.object));
      }
    }

    this.cache = newCache;

    const st = new n3.Writer().quadsToString(out);
    return st;
  }
}

export function ldesify(
  reader: Stream<string>,
  writer: Writer<string>,
  statePath?: string,
  check_properties: boolean = true,
  modifiedPath?: Term,
  isVersionOfPath?: Term,
) {
  let cache = {};
  if (statePath) cache = getPrevState(statePath);
  const transformer = new Transformer(
    modifiedPath || MODIFIED,
    isVersionOfPath || IS_VERSION_OF,
    cache,
  );

  reader.on("end", () => {
    if (statePath) {
      writeFileSync(statePath, transformer.save(), { encoding: "utf8" });
    }
  });

  reader.data((x) => {
    const st = check_properties
      ? transformer.transformCheckState(x)
      : transformer.simpleTransform(x);
    return writer.push(st);
  });
}
