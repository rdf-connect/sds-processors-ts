import { createBucketizerLD, FACTORY } from "@treecg/bucketizers";
import { writeFileSync } from "fs";
import { readFile } from "fs/promises";
import { Quad, Parser, Store, DataFactory } from "n3";
import * as N3 from "n3";
import { literal, NBNode, SR, SW, transformMetadata } from "./core.js";
import { Cleanup } from './exitHandler.js';
import { LDES, PPLAN, PROV, RDF, SDS } from "@treecg/types";
import { Stream, Writer } from "@treecg/connector-types";

type Data = { "data": Quad[], "metadata": Quad[] };
const { namedNode, quad } = DataFactory;


async function readState(path: string): Promise<any | undefined> {
  try {
    const str = await readFile(path, { "encoding": "utf-8" });
    return JSON.parse(str);
  } catch (e) {
    return
  }
}

async function writeState(path: string, content: any): Promise<void> {
  if (path) {
    const str = JSON.stringify(content);
    writeFileSync(path, str, { encoding: "utf-8" })
  }
}

function addProcess(id: NBNode | undefined, store: Store, strategyId: NBNode, bucketizeConfig: Quad[]): NBNode {
  const newId = store.createBlankNode();
  const time = new Date().getTime();

  store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
  store.addQuad(newId, RDF.terms.type, LDES.terms.Bucketization);

  store.addQuads(bucketizeConfig);

  store.addQuad(newId, PROV.terms.startedAtTime, literal(time));
  store.addQuad(newId, PROV.terms.used, strategyId);
  if (id)
    store.addQuad(newId, PROV.terms.used, id);

  return newId;
}

function parseQuads(quads: string | Quad[]): Quad[] {
  if (quads instanceof Array) return <Quad[]><any>quads;
  console.log("Parsing quads!", quads);
  const parser = new N3.Parser();
  return parser.parse(quads);
}

function serializeQuads(quads: Quad[]): string {
  const writer = new N3.Writer();
  return writer.quadsToString(quads);
}

export async function doTheBucketization(
  dataReader: Stream<string>,
  metadataReader: Stream<string>,
  dataWriter: Writer<string>,
  metadataWriter: Writer<string>,
  location: string,
  savePath: string,
  sourceStream: string | undefined,
  resultingStream: string
) {
  dataReader.on("end", () => {
    console.log("bucketize data closed");
    dataWriter.end()
  });
  metadataReader.on("end", () => { console.log("buckeitze index closed"); metadataWriter.end() });

  const sr = { metadata: metadataReader, data: dataReader };
  const sw = { metadata: metadataWriter, data: dataWriter };

  const content = await readFile(location, { encoding: "utf-8" });
  console.log("content", content);
  const quads = new Parser().parse(content);

  const quadMemberId = <NBNode>quads.find(quad =>
    quad.predicate.equals(RDF.terms.type) && quad.object.equals(LDES.terms.BucketizeStrategy)
  )!.subject;
  console.log("Do bucketization", quadMemberId);

  const f = transformMetadata(
    namedNode(resultingStream),
    sourceStream ? namedNode(sourceStream) : undefined,
    "sds:Member",
    (x, y) => addProcess(x, y, quadMemberId, quads)
  );
  sr.metadata.data(
    quads => sw.metadata.push(serializeQuads(f(parseQuads(quads))))
  );

  if (sr.metadata.lastElement) {
    sw.metadata.push(serializeQuads(f(parseQuads(sr.metadata.lastElement))));
  }

  const state = await readState(savePath);

  const bucketizer = FACTORY.buildLD(quads, quadMemberId, state);

  if (state)
    bucketizer.importState(state);

  // Cleanup(async () => {
  //     const state = bucketizer.exportState()
  //     await writeState(savePath, state);
  // })

  sr.data.data(async (data: Quad[] | string) => {
    const t = parseQuads(data);
    console.log("Bucketizing member")
    if (!t.length) return;

    const checkStream = sourceStream ? (q: Quad) => t.some(q2 => q2.subject.equals(q.subject) && q2.predicate.equals(SDS.terms.stream) && q2.object.value === sourceStream) : (_: Quad) => true;

    const members = [...new Set(t.filter(q => q.predicate.equals(SDS.terms.custom("payload")) && checkStream(q)).map(q => q.object))];
    if (members.length > 1) {
      console.error("Detected more members ids than expected");
    }

    if (members.length === 0) return;

    const sub = members[0].value;
    const extras = <Quad[]><unknown>bucketizer.bucketize(t, sub);

    const recordId = extras.find(q => q.predicate.equals(SDS.terms.payload))!.subject;
    t.push(...<Quad[]>extras);
    t.push(quad(recordId, SDS.terms.stream, namedNode(resultingStream)));
    t.push(quad(recordId, RDF.terms.type, SDS.terms.Member));

    console.log("Pushing thing bucketized!")
    await sw.data.push(serializeQuads(t));
  });
}

