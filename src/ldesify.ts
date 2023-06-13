import { Stream, Writer } from "@treecg/connector-types";
import { readFileSync, writeFileSync } from "fs";
import * as n3 from "n3";


const { namedNode, quad, literal } = n3.DataFactory;

type Prop = {
  pred: n3.Term,
  value: n3.Term,
  subj: n3.Term
};

type StateDict = { [identify: string]: n3.Quad[] };
type StateCache = { [identify: string]: string };

function getPrevState(path: string): StateCache {
  try {
    return JSON.parse(readFileSync(path).toString());
  } catch (ex: any) {
    return {}
  }
}

function createCache(quads: n3.Quad[], keys: string[]): string {
  let out = "";
  for (let key of keys) {
    const v = quads.find(v => v.predicate.value === key);
    if (v) {
      out += v.object.value;
    } else {
      out += "undefined";
    }
    out += "+";
  }

  return out.slice(0, -1);
}

const IS_VERSION_OF = namedNode("http://purl.org/dc/terms/isVersionOf")
const MODIFIED = namedNode("http://purl.org/dc/terms/modified")
const TIMESTAMP = namedNode("http://www.w3.org/2001/XMLSchema#dateTime");

export function ldes_transform(input: string, path: string) {
  const quads = new n3.Parser().parse(input);

  const preState = getPrevState(path);
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

    if (newCache[k] === preState[k]) {
      continue;
    }

    const subject = v[0].subject;

    const date = new Date().toISOString();
    const sub = namedNode(v[0].subject.value +"#" + date)

    out.push(
      quad(sub, MODIFIED, literal(date, TIMESTAMP)), // VersionOf
      quad(sub, IS_VERSION_OF, subject), // Timestamp
    );

    for (let q of v) {
      out.push(quad(sub, q.predicate, q.object));
    }
  }


  writeFileSync(path, JSON.stringify(newCache));
  const st = new n3.Writer().quadsToString(out);
  return st;
}

export function ldesify(reader: Stream<string>, writer: Writer<string>, statePath: string) {
  reader.data(x => {
    const st = ldes_transform(x, statePath);
    return writer.push(st);
  });
}
