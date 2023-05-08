import { Stream, Writer } from "@treecg/connector-types";
import { RDF as RDFT, SDS } from "@treecg/types";
import type * as RDF from '@rdfjs/types';
import { blankNode, namedNode } from "./core";
import { Writer as NWriter, Parser, DataFactory, Store } from "n3";

class Tracker {
  max: number;
  at: number = 0;
  logged: number = 0;

  constructor(max: number) {
    this.max = max;
  }

  inc() {
    this.at += 1;

    let at = Math.round(this.at * 100 / this.max);
    if (at > this.logged) {
      console.log(at, "%");
      this.logged = at;
    }

  }
}

function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
  if (typeof data === 'string' || data instanceof String) {
    const parse = new Parser();
    return parse.parse(<string>data);
  } else {
    return data
  }
}

function extractMember(store: Store, subject: string): RDF.Quad[] {
  const subGraph: RDF.Quad[] = [];
  // Extract forward relations recursively
  // TODO: deal with backwards relations
  // TODO: deal with cycles
  for(const quad of store.getQuads(subject, null, null, null)) {
    if(quad.object.termType === "NamedNode") {
      subGraph.push(...extractMember(store, quad.object.id));
    }
    subGraph.push(quad);
  }
  return subGraph;
}

export function sdsify(input: Stream<string | RDF.Quad[]>, output: Writer<string>, stream: string, type?: string) {
  const streamNode = namedNode(stream);

  input.data(async input => {
    const quads = maybe_parse(input);
    console.log("sdsify: Got input", quads.length, "quads");

    const members: { [id: string]: RDF.Quad[] } = {};

    if (type) {
      // Group quads based on given member type
      const store = new Store(quads);
      for(const quad of store.getQuads(null, RDFT.terms.type, type, null)) {
        members[quad.subject.value] = extractMember(store, quad.subject.value);
      }
    } else {
      // Group quads based on Subject IRI
      for (let quad of quads) {
        if (!members[quad.subject.value]) {
          members[quad.subject.value] = [];
        }
        members[quad.subject.value].push(quad);
      }
    }


    let membersCount = 0;

    let first = true;

    for (let key of Object.keys(members)) {
      const quads = members[key];
      if (first) {
        first = false;
        console.log("predicates", quads.map(q => q.predicate.value));
      }
      const blank = blankNode();
      quads.push(
        DataFactory.quad(
          blank,
          SDS.terms.payload,
          namedNode(key),
        ),
        DataFactory.quad(
          blank,
          SDS.terms.stream,
          streamNode,
        ),
      );

      const str = new NWriter().quadsToString(quads);
      await output.push(str);
      membersCount += 1;
    }

    console.log("sdsify: pushed ", membersCount, "members");

  });
}
