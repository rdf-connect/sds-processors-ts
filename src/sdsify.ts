import type { Stream, Writer } from "@ajuvercr/js-runner";
import { Logger, RDF as RDFT, SDS } from "@treecg/types";
import type * as RDF from "@rdfjs/types";
import { blankNode, namedNode } from "./core.js";
import { DataFactory, Parser, Quad_Object, Store, Writer as NWriter } from "n3";
import { Term } from "@rdfjs/types";

const logger = new Logger("info", "info");

function maybe_parse(data: RDF.Quad[] | string): RDF.Quad[] {
  if (typeof data === "string" || data instanceof String) {
    const parse = new Parser();
    return parse.parse(<string>data);
  } else {
    return data;
  }
}

function extractMember(store: Store, subject: RDF.Term): RDF.Quad[] {
  const subGraph: RDF.Quad[] = [];
  // Extract forward relations recursively
  // TODO: deal with backwards relations
  // TODO: deal with cycles
  for (const quad of store.getQuads(subject, null, null, null)) {
    if (
      quad.object.termType === "NamedNode" ||
      quad.object.termType === "BlankNode"
    ) {
      subGraph.push(...extractMember(store, quad.object));
    }
    subGraph.push(quad);
  }
  return subGraph;
}

export function sdsify(
  input: Stream<string | RDF.Quad[]>,
  output: Writer<string>,
  streamNode: Term,
  type?: Term,
) {
  input.data(async (input) => {
    const quads = maybe_parse(input);
    const members: { [id: string]: RDF.Quad[] } = {};

    if (type) {
      // Group quads based on given member type
      const store = new Store(quads);
      for (const quad of store.getQuads(null, RDFT.terms.type, type, null)) {
        members[quad.subject.value] = extractMember(store, quad.subject);
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
      }
      const blank = blankNode();
      quads.push(
        DataFactory.quad(blank, SDS.terms.payload, namedNode(key)),
        DataFactory.quad(blank, SDS.terms.stream, <Quad_Object>streamNode),
      );

      const str = new NWriter().quadsToString(quads);
      await output.push(str);
      membersCount += 1;
    }
  });

  input.on("end", () => {
    console.log("sdsify closed down");
    output.end();
  });
}
