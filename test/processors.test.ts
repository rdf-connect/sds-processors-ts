import { describe, expect, test } from "vitest";
import {
    FullProc,
    Processor,
    ReaderInstance,
    Runner,
    WriterInstance,
} from "@rdfc/js-runner";
import { logger, TestClient } from "@rdfc/js-runner/lib/testUtils";
import { NamedNode, Parser, Writer } from "n3";
import { extractShapes } from "rdf-lens";
import { OrchestratorMessage } from "@rdfc/js-runner/lib/reexports";
import { Quad, Term } from "@rdfjs/types";
import { readFile } from "fs/promises";
import { createTermNamespace } from "@treecg/types";

import {
    Bucketizer,
    Generator,
    LdesDiskWriter,
    Ldesify,
    LdesifySDS,
    MemberAsNamedGraph,
    Sdsify,
    Shapify,
    StreamJoin,
} from "../lib/";

const shapeQuads = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
[ ] a sh:NodeShape;
  sh:targetClass <JsProcessorShape>;
  sh:property [
    sh:path rdfc:entrypoint;
    sh:name "location";
    sh:minCount 1;
    sh:maxCount 1;
    sh:datatype xsd:string;
  ], [
    sh:path rdfc:file;
    sh:name "file";
    sh:minCount 1;
    sh:maxCount 1;
    sh:datatype xsd:string;
  ], [
    sh:path rdfc:class;
    sh:name "clazz";
    sh:maxCount 1;
    sh:datatype xsd:string;
  ].
`;
const OWL = createTermNamespace("http://www.w3.org/2002/07/owl#", "imports");
const processorShapes = extractShapes(new Parser().parse(shapeQuads));
const base = "https://w3id.org/rdf-connect#";

export async function importFile(file: string): Promise<Quad[]> {
    const done = new Set<string>();
    const todo = [new URL("file://" + file)];
    const quads: Quad[] = [];

    let item = todo.pop();
    while (item !== undefined) {
        if (done.has(item.toString())) {
            item = todo.pop();
            continue;
        }
        done.add(item.toString());
        if (item.protocol !== "file:") {
            throw "No supported protocol " + item.protocol;
        }

        const txt = await readFile(item.pathname, { encoding: "utf8" });
        const extras = new Parser({ baseIRI: item.toString() }).parse(txt);

        for (const o of extras
            .filter(
                (x) =>
                    x.subject.value === item?.toString() &&
                    x.predicate.equals(OWL.imports),
            )
            .map((x) => x.object.value)) {
            todo.push(new URL(o));
        }
        quads.push(...extras);

        item = todo.pop();
    }

    return quads;
}

export async function getProc<T extends Processor<unknown>>(
    config: string,
    ty: string,
    configLocation: string,
    uri = "http://example.com/ns#processor",
): Promise<FullProc<T>> {
    const configQuads = await importFile(configLocation);
    const procConfig = processorShapes.lenses["JsProcessorShape"].execute({
        id: new NamedNode(base + ty),
        quads: configQuads,
    });

    const msgs: OrchestratorMessage[] = [];
    const write = async (x: OrchestratorMessage) => {
        msgs.push(x);
    };
    const runner = new Runner(
        new TestClient(),
        write,
        "http://example.com/ns#",
        logger,
    );
    configQuads.push(...new Parser().parse(config));
    await runner.handleOrchMessage({
        pipeline: new Writer().quadsToString(configQuads),
    });

    const proc = await runner.addProcessor<T>({
        config: JSON.stringify(procConfig),
        arguments: "",
        uri,
    });

    return proc;
}

async function checkProcDefinition(file: string, n: string) {
    const quads = await importFile(file);
    const procConfig = <{ file: Term; location: string; clazz: string }>(
        processorShapes.lenses["JsProcessorShape"].execute({
            id: new NamedNode(base + n),
            quads: quads,
        })
    );
    expect(procConfig.file, n + " has file").toBeDefined();
    expect(procConfig.location, n + " has location").toBeDefined();
    expect(procConfig.clazz, n + " has clazz").toBeDefined();
}

describe("SDS processors tests", async () => {
    test("js:Bucketize is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <jr>;
    rdfc:metadataInput <jr>;
    rdfc:dataOutput <jw>;
    rdfc:metadataOutput <jw>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:SubjectFragmentation;
    tree:fragmentationPath ( );
  ] [
    a tree:PageFragmentation;
    tree:pageSize 2;
  ] );
      rdfc:inputStreamId <http://testStream>;
      rdfc:outputStreamId <http://newStream>.
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        await checkProcDefinition(configLocation, "Bucketize");

        const bucketizer = await getProc<Bucketizer>(
            processor,
            "Bucketize",
            configLocation,
        );
        await bucketizer.init();

        expect(bucketizer.prefix).toBe("root"); // default
        expect(bucketizer.channels.dataInput).toBeInstanceOf(ReaderInstance);
        expect(bucketizer.channels.metadataInput).toBeInstanceOf(
            ReaderInstance,
        );
        expect(bucketizer.channels.dataOutput).toBeInstanceOf(WriterInstance);
        expect(bucketizer.channels.metadataOutput).toBeInstanceOf(
            WriterInstance,
        );
        expect(bucketizer.savePath).toBeUndefined();
        expect(bucketizer.sourceStream?.value).toBe("http://testStream");
        expect(bucketizer.resultingStream?.value).toBe("http://newStream");
        expect(bucketizer.config.length).toBe(2);
    });

    test("js:Ldesify is properly defined", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

    <http://example.com/ns#processor> a rdfc:Ldesify;
      rdfc:input <jr>;
      rdfc:path "save.json";
      rdfc:output <jw>;
      rdfc:checkProps true;
      rdfc:timestampPath <http://example.com/ns#time>;
      rdfc:versionOfPath <http://example.com/ns#ver>;
      .
    `;

        const configLocation = process.cwd() + "/configs/ldesify.ttl";
        await checkProcDefinition(configLocation, "Ldesify");

        const processor = await getProc<Ldesify>(
            processorConfig,
            "Ldesify",
            configLocation,
        );
        await processor.init();

        expect(processor.reader).toBeInstanceOf(ReaderInstance);
        expect(processor.writer).toBeInstanceOf(WriterInstance);
        expect(processor.statePath).toBe("save.json");
        expect(processor.modifiedPath?.value).toBe(
            "http://example.com/ns#time",
        );
        expect(processor.isVersionOfPath?.value).toBe(
            "http://example.com/ns#ver",
        );
        expect(processor.check_properties).toBe(true);
    });

    test("js:LdesifySDS is properly defined", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

    <http://example.com/ns#processor> a rdfc:LdesifySDS;
      rdfc:input <jr>;
      rdfc:statePath "save.json";
      rdfc:output <jw>;
      rdfc:checkProps true;
      rdfc:timestampPath <http://example.com/ns#time>;
      rdfc:versionOfPath <http://example.com/ns#ver>;
      rdfc:targetStream <http://example.com/ns#target>;
      rdfc:sourceStream <http://example.com/ns#source>;
      .
    `;

        const configLocation = process.cwd() + "/configs/ldesify.ttl";
        await checkProcDefinition(configLocation, "LdesifySDS");

        const processor = await getProc<LdesifySDS>(
            processorConfig,
            "LdesifySDS",
            configLocation,
        );
        await processor.init();

        expect(processor.reader).toBeInstanceOf(ReaderInstance);
        expect(processor.writer).toBeInstanceOf(WriterInstance);
        expect(processor.statePath).toBe("save.json");
        expect(processor.modifiedPathM?.value).toBe(
            "http://example.com/ns#time",
        );
        expect(processor.isVersionOfPathM?.value).toBe(
            "http://example.com/ns#ver",
        );
        expect(processor.targetStream.value).toBe(
            "http://example.com/ns#target",
        );
        expect(processor.sourceStream?.value).toBe(
            "http://example.com/ns#source",
        );
    });

    test("generator", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

    <http://example.com/ns#processor>  a rdfc:Generate;
      rdfc:count 5;
      rdfc:waitMS 500;
      rdfc:timestampPath <http://out.com#out>;
      rdfc:output <jw>.
    `;

        const configLocation = process.cwd() + "/configs/generator.ttl";
        await checkProcDefinition(configLocation, "Generate");

        const processor = await getProc<Generator>(
            processorConfig,
            "Generate",
            configLocation,
        );
        await processor.init();

        expect(processor.writer).toBeInstanceOf(WriterInstance);
        expect(processor.wait).toBe(500);
        expect(processor.timestampPath?.value).toBe("http://out.com#out");
        expect(processor.count).toBe(5);
    });

    test("sdsify", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

    <http://example.com/ns#processor> a rdfc:Sdsify;
        rdfc:input <jr>;
        rdfc:output <jw>;
        rdfc:stream <http://me.com/stream>;
        rdfc:typeFilter <http://ex.org/Type>, <http://ex.org/AnotherType>;
        rdfc:timestampPath <http://ex.org/timestamp>;
        rdfc:shape """
          @prefix sh: <http://www.w3.org/ns/shacl#>.
          @prefix ex: <http://ex.org/>.

          [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass.
        """.
      `;

        const configLocation = process.cwd() + "/configs/sdsify.ttl";
        await checkProcDefinition(configLocation, "Sdsify");

        const processor = await getProc<Sdsify>(
            processorConfig,
            "Sdsify",
            configLocation,
        );
        await processor.init();

        expect(processor.input).toBeInstanceOf(ReaderInstance);
        expect(processor.output).toBeInstanceOf(WriterInstance);
        expect(processor.streamNode.value).toBe("http://me.com/stream");
        expect(processor.timestampPath?.value).toBe("http://ex.org/timestamp");
        expect(processor.types?.map((x) => x.value)).toEqual([
            "http://ex.org/Type",
            "http://ex.org/AnotherType",
        ]);
    });

    test("streamJoin", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
      <http://example.com/ns#processor> a rdfc:StreamJoin;
        rdfc:input <jr>, <jr2>;
        rdfc:output <jw>.
      `;

        const configLocation = process.cwd() + "/configs/stream_join.ttl";
        await checkProcDefinition(configLocation, "StreamJoin");

        const processor = await getProc<StreamJoin>(
            processorConfig,
            "StreamJoin",
            configLocation,
        );
        await processor.init();

        expect(processor.inputs.length).toBe(2);
        for (const i of processor.inputs) {
            expect(i).toBeInstanceOf(ReaderInstance);
        }
        expect(processor.output).toBeInstanceOf(WriterInstance);
    });

    test("memberAsNamedGraph", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
      <http://example.com/ns#processor> a rdfc:MemberAsNamedGraph;
        rdfc:input <jr>;
        rdfc:output <jw>.
      `;

        const configLocation = process.cwd() + "/configs/member_as_graph.ttl";
        await checkProcDefinition(configLocation, "MemberAsNamedGraph");

        const processor = await getProc<MemberAsNamedGraph>(
            processorConfig,
            "MemberAsNamedGraph",
            configLocation,
        );
        await processor.init();

        expect(processor.input).toBeInstanceOf(ReaderInstance);
        expect(processor.output).toBeInstanceOf(WriterInstance);
    });

    test("js:LdesDiskWriter is properly defined", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
        <http://example.com/ns#processor> a rdfc:LdesDiskWriter;
            rdfc:dataInput <jr>;
            rdfc:metadataInput <jr>;
            rdfc:directory "/tmp/ldes-disk/".
        `;

        const configLocation = process.cwd() + "/configs/ldes_disk_writer.ttl";
        await checkProcDefinition(configLocation, "LdesDiskWriter");

        const processor = await getProc<LdesDiskWriter>(
            processorConfig,
            "LdesDiskWriter",
            configLocation,
        );
        await processor.init();
        expect(processor.data).toBeInstanceOf(ReaderInstance);
        expect(processor.metadata).toBeInstanceOf(ReaderInstance);
        expect(processor.directory).toBe("/tmp/ldes-disk/");
    });

    test("rdfc:Shapify is properly defined", async () => {
        const processorConfig = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:Shapify;
            rdfc:input <jr>;
            rdfc:output <jr>;
            rdfc:shape <MyShape>.
        `;

        const configLocation = process.cwd() + "/configs/shapify.ttl";
        await checkProcDefinition(configLocation, "Shapify");

        const processor = await getProc<Shapify>(
            processorConfig,
            "Shapify",
            configLocation,
        );
        await processor.init();
        expect(processor.reader).toBeInstanceOf(ReaderInstance);
        expect(processor.writer).toBeInstanceOf(WriterInstance);
        expect(processor.shape).toBeDefined();
        expect(processor.shape.id.value).toBe("MyShape");
    });
});
