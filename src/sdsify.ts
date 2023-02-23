import { Stream, Writer } from "@treecg/connector-types";
import { RDF as RDFT, SDS } from "@treecg/types";
import type * as RDF from '@rdfjs/types';
import { blankNode, namedNode } from "./core";
import { Writer as NWriter, Parser, DataFactory } from "n3";

class Tracker {
  max: number;
  at: number = 0;
  logged: number = 0;

  constructor(max:number) {
    this.max = max;
  }

  inc() {
    this.at+=1;

    let at = Math.round(this.at * 100 / this.max);
    if(at > this.logged) {
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

export function sdsify(input: Stream<string | RDF.Quad[]>, output: Writer<string>, stream: string, type?: string) {
  const streamNode = namedNode(stream);

  input.data(async input => {
    const quads = maybe_parse(input);
    console.log("Got input", quads.length, "quads");
    const per_subject: {[id: string]: RDF.Quad[]} = {};
    const tracker = new Tracker(quads.length);
    for(let quad of quads) {
      if(!per_subject[quad.subject.value]) {
        per_subject[quad.subject.value] = [];
      }
      per_subject[quad.subject.value].push(quad);
      tracker.inc();
    }

    for(let key of Object.keys(per_subject)) {
      const quads = per_subject[key];
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
    }

  });
}
