import type { Stream, Writer } from "@ajuvercr/js-runner";
import { Logger, RDF as RDFT, SDS } from "@treecg/types";
import type * as RDF from "@rdfjs/types";
import { blankNode, namedNode } from "./core.js";
import { DataFactory, Parser, Quad_Object, Store, Writer as NWriter } from "n3";
import { Term } from "@rdfjs/types";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { RdfStore } from "rdf-stores";

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

function getExtractor(shape?: {
  id: Term;
  quads: RDF.Quad[];
}): CBDShapeExtractor {
  if (!shape) return new CBDShapeExtractor();

  const store = RdfStore.createDefault();
  shape.quads.forEach((x) => store.addQuad(x));
  return new CBDShapeExtractor(store, undefined, {
    fetch: async (_) =>
      new Response("", { headers: { "content-type": "text/turtle" } }),
  });
}

export function sdsify(
  input: Stream<string | RDF.Quad[]>,
  output: Writer<string>,
  streamNode: Term,
  type?: Term,
  shape?: {
    id: Term;
    quads: RDF.Quad[];
  },
) {
  const extractor: CBDShapeExtractor = getExtractor(shape);

  input.data(async (input) => {
    const quads = maybe_parse(input);
    const members: { [id: string]: RDF.Quad[] } = {};
    const store = RdfStore.createDefault();
    quads.forEach((x) => store.addQuad(x));

    if (type) {
      console.log("Using type", type.value, "shape", shape?.id.value);
      // Group quads based on given member type
      for (const quad of store.getQuads(null, RDFT.terms.type, type, null)) {
        members[quad.subject.value] = await extractor.extract(
          store,
          quad.subject,
          shape?.id,
        );
      }
    } else {
      // Group quads based on Subject IRI
      for (let quad of quads) {
        if (
          quad.subject.termType === "NamedNode" &&
          !members[quad.subject.value]
        ) {
          members[quad.subject.value] = members[quad.subject.value] =
            await extractor.extract(store, quad.subject, shape?.id);
        }
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
        DataFactory.quad(
          blank,
          SDS.terms.payload,
          namedNode(key),
          SDS.terms.custom("DataDescription"),
        ),
        DataFactory.quad(
          blank,
          SDS.terms.stream,
          <Quad_Object>streamNode,
          SDS.terms.custom("DataDescription"),
        ),
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
