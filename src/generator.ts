import type { Writer } from "@rdfc/js-runner";
import { randomInt } from "crypto";

import { DataFactory } from "n3";
import * as N3 from "n3";

import { Term } from "@rdfjs/types";

const { namedNode, literal, quad } = DataFactory;
const NS = "http://time.is/ns#";

const types = [
  { x: 2, y: 4 },
  { x: 3, y: 4 },
  { x: 2, y: 2 },
  { x: 3, y: 3 },
  { x: 4, y: 4 },
  { x: 5, y: 5 },
  { x: 6, y: 6 },
];

function generateMember(i: number, timestampPath?: Term) {
  const id = namedNode(NS + i);
  const q = (p: string, o: string) => quad(id, namedNode(p), literal(o));

  const { x, y } = types[i % types.length];

  const quads = [
    q(NS + "x", x + ""),
    q(NS + "y", y + ""),
    q(NS + "v", randomInt(100) + ""),
  ];

  if (timestampPath) {
    quads.push(
      quad(id, <N3.Quad_Predicate>timestampPath, literal(Date.now() + "")),
    );
  }

  return new N3.Writer().quadsToString(quads);
}

export async function generate(
  writer: Writer<string>,
  mCount?: number,
  mWait?: number,
  timestampPath?: Term,
) {
  const count = mCount ?? 100000;
  const wait = mWait ?? 50.0;

  return async function () {
    console.log(`generate starting`);

    for (let i = 0; i < count; i++) {
      console.log(`${i}/${count}`);
      await writer.push(generateMember(i, timestampPath));
      await new Promise((res) => setTimeout(res, wait));
    }
  };
}
