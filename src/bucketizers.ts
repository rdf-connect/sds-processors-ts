import { FACTORY } from "@treecg/bucketizers";
import { readFileSync, writeFileSync } from "fs";
import { readFile } from "fs/promises";
import { DataFactory, Parser, Store } from "n3";
import * as N3 from "n3";
import { blankNode, getLatestShape, getLatestStream, literal, SR, SW, transformMetadata } from "./core";
import { LDES, PPLAN, PROV, RDF, SDS } from "@treecg/types";
import type { Stream, Writer } from "@ajuvercr/js-runner";
import { BucketizerConfig, BucketizerOrchestrator } from "./bucketizers/index";
import { Quad, Quad_Object, Term } from "rdf-js";
import { Bucket, Extractor, Record } from "./utils/index";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { Cleanup } from "./exitHandler";
import { RdfStore } from "rdf-stores";

type Data = { data: Quad[]; metadata: Quad[] };
const { namedNode, quad } = DataFactory;

async function readState(path: string): Promise<any | undefined> {
  try {
    const str = await readFile(path, { encoding: "utf-8" });
    return JSON.parse(str);
  } catch (e) {
    return;
  }
}

async function writeState(
  path: string | undefined,
  content: string,
): Promise<void> {
  if (path) {
    writeFileSync(path, content, { encoding: "utf-8" });
  }
}

function addProcess(
  id: Term | undefined,
  store: Store,
  strategyId: Term,
  bucketizeConfig: Quad[],
): Term {
  const newId = store.createBlankNode();
  const time = new Date().toISOString();

  store.addQuad(newId, RDF.terms.type, PPLAN.terms.Activity);
  store.addQuad(newId, RDF.terms.type, LDES.terms.Bucketization);

  store.addQuads(bucketizeConfig);

  store.addQuad(newId, PROV.terms.startedAtTime, literal(time));
  store.addQuad(newId, PROV.terms.used, <Quad_Object>strategyId);
  if (id) {
    store.addQuad(newId, PROV.terms.used, <Quad_Object>id);
  }

  return newId;
}

function parseQuads(quads: string | Quad[]): Quad[] {
  if (quads instanceof Array) return <Quad[]>(<any>quads);
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
  resultingStream: string,
) {
  dataReader.on("end", () => {
    console.log("bucketize data closed");
    dataWriter.end();
  });
  metadataReader.on("end", () => {
    console.log("buckeitze index closed");
    metadataWriter.end();
  });

  const sr = { metadata: metadataReader, data: dataReader };
  const sw = { metadata: metadataWriter, data: dataWriter };

  const content = await readFile(location, { encoding: "utf-8" });
  const quads = new Parser().parse(content);

  const quadMemberId = quads.find(
    (quad) =>
      quad.predicate.equals(RDF.terms.type) &&
      quad.object.equals(LDES.terms.BucketizeStrategy),
  )!.subject;

  const f = transformMetadata(
    namedNode(resultingStream),
    sourceStream ? namedNode(sourceStream) : undefined,
    "sds:Member",
    (x, y) => addProcess(x, y, quadMemberId, quads),
  );
  sr.metadata.data((quads) =>
    sw.metadata.push(serializeQuads(f(parseQuads(quads)))),
  );

  if (sr.metadata.lastElement) {
    sw.metadata.push(serializeQuads(f(parseQuads(sr.metadata.lastElement))));
  }

  const state = await readState(savePath);

  const bucketizer = FACTORY.buildLD(quads, quadMemberId, state);

  if (state) {
    bucketizer.importState(state);
  }

  // Cleanup(async () => {
  //     const state = bucketizer.exportState()
  //     await writeState(savePath, state);
  // })

  sr.data.data(async (data: Quad[] | string) => {
    const t = parseQuads(data);
    if (!t.length) return;

    const checkStream = sourceStream
      ? (q: Quad) =>
          t.some(
            (q2) =>
              q2.subject.equals(q.subject) &&
              q2.predicate.equals(SDS.terms.stream) &&
              q2.object.value === sourceStream,
          )
      : (_: Quad) => true;

    const members = [
      ...new Set(
        t
          .filter(
            (q) =>
              q.predicate.equals(SDS.terms.custom("payload")) && checkStream(q),
          )
          .map((q) => q.object),
      ),
    ];
    if (members.length > 1) {
      console.error("Detected more members ids than expected");
    }

    if (members.length === 0) return;

    const sub = members[0].value;
    const extras = <Quad[]>(<unknown>bucketizer.bucketize(t, sub));

    const recordId = extras.find((q) =>
      q.predicate.equals(SDS.terms.payload),
    )!.subject;
    t.push(...(<Quad[]>extras));
    t.push(quad(recordId, SDS.terms.stream, namedNode(resultingStream)));
    t.push(quad(recordId, RDF.terms.type, SDS.terms.Member));

    await sw.data.push(serializeQuads(t));
  });
}

type Channels = {
  dataInput: Stream<string>;
  metadataInput: Stream<string>;
  dataOutput: Writer<string>;
  metadataOutput: Writer<string>;
};

type Config = {
  quads: { id: Term; quads: Quad[] };
  strategy: BucketizerConfig[];
};

function record_to_quads(
  record: Record,
  resultingStream: Term,
  buckets: Bucket[],
): Quad[] {
  const id = blankNode();
  const out: Quad[] = [
    quad(id, SDS.terms.payload, <N3.Quad_Object>record.data.id),
    quad(id, SDS.terms.stream, <N3.Quad_Object>resultingStream),
    ...record.data.quads,
    ...buckets
      .map((bucket) => bucket.id)
      .map((bucket) => quad(id, SDS.terms.bucket, <N3.Quad_Object>bucket)),
  ];
  return out;
}

function bucket_to_quads(bucket: Bucket): Quad[] {
  const out: Quad[] = [
    quad(
      <N3.Quad_Subject>bucket.id,
      RDF.terms.type,
      SDS.terms.custom("Bucket"),
    ),
  ];
  out.push(
    quad(
      <N3.Quad_Subject>bucket.id,
      SDS.terms.custom("immutable"),
      literal((bucket.immutable || false) + ""),
    ),
  );

  if (bucket.root) {
    out.push(
      quad(
        <N3.Quad_Subject>bucket.id,
        SDS.terms.custom("isRoot"),
        literal("true"),
      ),
    );
  }

  for (let rel of bucket.links) {
    const id = blankNode();
    out.push(
      quad(<N3.Quad_Subject>bucket.id, SDS.terms.relation, id),
      quad(id, SDS.terms.relationType, <N3.Quad_Object>rel.type),
      quad(id, SDS.terms.relationBucket, <N3.Quad_Object>rel.target),
    );

    if (rel.path) {
      out.push(
        quad(id, SDS.terms.relationPath, <N3.Quad_Object>rel.path.id),
        ...rel.path.quads,
      );
    }

    if (rel.value) {
      out.push(quad(id, SDS.terms.relationValue, <N3.Quad_Object>rel.value));
    }

    out.push();
  }

  return out;
}

function set_metadata(
  channels: Channels,
  resultingStream: Term,
  sourceStream: Term | undefined,
  config: Config,
) {
  const f = transformMetadata(
    resultingStream,
    sourceStream,
    "sds:Member",
    (x, y) => addProcess(x, y, config.quads.id, config.quads.quads),
  );
  channels.metadataInput.data((quads) =>
    channels.metadataOutput.push(serializeQuads(f(parseQuads(quads)))),
  );
}

function read_save(savePath?: string) {
  try {
    if (savePath) {
      return readFileSync(savePath, { encoding: "utf8" });
    }
  } catch (ex: any) {}
  return;
}

export async function bucketize(
  channels: Channels,
  config: Config,
  savePath: string | undefined,
  sourceStream: Term | undefined,
  resultingStream: Term,
) {
  set_metadata(channels, resultingStream, sourceStream, config);
  const save = read_save(savePath);
  const orchestrator = new BucketizerOrchestrator(config.strategy, save);
  const extractor = new Extractor(new CBDShapeExtractor(), sourceStream);

  channels.metadataInput.data((x) => {
    const quads = new Parser().parse(x);

    const store = new Store();
    store.addQuads(quads);

    const latest = sourceStream || getLatestStream(store);
    const latestShape = !!latest ? getLatestShape(latest, store) : undefined;

    if(latestShape) {
      const rdfStore = RdfStore.createDefault();
      quads.forEach(x => rdfStore.addQuad(x));
      const cbd_extract = new CBDShapeExtractor(rdfStore);

      extractor.extractor = cbd_extract;
      extractor.shape = latestShape;

    }

  });

  Cleanup(async () => {
    const state = orchestrator.save();
    await writeState(savePath, state);
  });

  const buckets: { [id: string]: Bucket } = {};
  channels.dataInput.data(async (x) => {
    const outputQuads: Quad[] = [];
    const quads = new Parser().parse(x);
    // Strange, this should be doable with just shacl shape definitions
    // But it is a good question to ask, what if an sds:Record is not only cbd?
    const records = await extractor.parse_records(quads);
    const relatedBuckets = new Set<string>();
    for (let record of records) {
      const record_buckets = orchestrator.bucketize(
        record,
        buckets,
        sourceStream?.value || "root",
      );
      outputQuads.push(
        ...record_to_quads(
          record,
          resultingStream,
          record_buckets.map((x) => buckets[x]),
        ),
      );

      for (let b of record_buckets) {
        relatedBuckets.add(b);
        let parent = buckets[b].parent;
        while (parent) {
          relatedBuckets.add(parent.id.value);
          parent = parent.parent;
        }
      }
    }

    for (let relatedBucket of relatedBuckets.values()) {
      outputQuads.push(...bucket_to_quads(buckets[relatedBucket]));
    }

    await channels.dataOutput.push(new N3.Writer().quadsToString(outputQuads));
  });
}
