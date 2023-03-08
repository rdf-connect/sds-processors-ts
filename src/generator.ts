import { Writer } from "@treecg/connector-types";
import { randomInt } from "crypto";

import { DataFactory } from 'n3';
import * as N3 from 'n3';

const { namedNode, literal, quad } = DataFactory;
const NS = "http://time.is/ns#";

const types = [
  { x: 2, y: 4 },
  { x: 3, y: 4 },
  { x: 2, y: 2 },
  { x: 3, y: 3 },
];

function generateMember(i: number, includeT: boolean) {
  const id = namedNode(NS + i);
  const q = (p: string, o: string) => quad(id, namedNode(p), literal(o));

  const { x, y } = types[i % types.length];

  const quads = [
    q(NS + "x", x + ""),
    q(NS + "y", y + ""),
    q(NS + "v", randomInt(100) + ""),
  ];

  if(includeT) {
    quads.push(q(NS+"time", Date.now() + ""));
  }

  return new N3.Writer().quadsToString(quads);
}


export async function generate(writer: Writer<string>, countstr?: string, waitstr?: string, withTimestamp?: string) {
  // const withT = withTimestamp ? withTimestamp.toLowerCase() === "true" : false;
  const withT = true;

  (async function() {
    console.log(`generate starting`);
    const count = countstr ? parseInt(countstr) : 100000;
    const wait = waitstr ? parseFloat(waitstr) : 50.0;

    for (let i = 0; i < count; i++) {
      console.log(`${i}/${count}`);
      await writer.push(generateMember(i, withT));
      await new Promise(res => setTimeout(res, wait));
    }
  })();
}
