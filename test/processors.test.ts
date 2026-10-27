import { describe, expect, test } from "vitest";
import { ProcHelper } from "@rdfc/js-runner/lib/testUtils";
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
import {
    DumpFragmentation,
    HourFragmentation,
    PageFragmentation,
    RTreeFragmentation,
    SubjectFragmentation,
    TimeBucketTreeConfig,
    TimebasedFragmentation,
} from "../lib/bucketizers/index";

import { mkdirSync, mkdtempSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";

import type { FullProc } from "@rdfc/js-runner";
import { fromPath } from "@rdfc/utils-js";

async function getProc<T>(
    config: string,
    name: string,
    processor: string,
): Promise<FullProc<T>> {
    const procHelper = new ProcHelper<FullProc<T>>();
    await procHelper.importFile(process.cwd() + "/configs/" + config);
    await procHelper.importInline(
        resolve("pipeline.ttl"),
        `@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.
` + processor,
    );
    procHelper.getConfig(name);
    return await procHelper.getProcessor("http://example.com/ns#processor");
}

describe("SDS processors tests", async () => {
    test("rdfc:Bucketize with complex Subject and Page fragmentation is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:SubjectFragmentation;
    tree:fragmentationPath ( );
  ] [
    a tree:PageFragmentation;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        // Load processor semantic definition
        await procHelper.importFile(configLocation);
        // Load processor instance declaration
        await procHelper.importInline("pipeline.ttl", processor);

        // Get processor configuration
        procHelper.getConfig("Bucketize");

        // Instantiate processor from declared instance
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.prefix).toBe("root"); // default
        expect(bucketizer.channels.dataInput.uri).toContain("dataInput");
        expect(bucketizer.channels.dataOutput.uri).toContain("dataOutput");
        expect(bucketizer.savePath).toBeUndefined();
        expect(bucketizer.sourceStream?.value).toBe("http://testStream");
        // The resulting stream is the output channel
        await bucketizer.init();
        expect(bucketizer.resultingStream?.value).toBe(
            bucketizer.channels.dataOutput.uri,
        );
        expect(bucketizer.config.length).toBe(2);

        const subjectConfig = <SubjectFragmentation>bucketizer.config[0].config;
        expect(subjectConfig.path).toBeDefined();

        const pageConfig = <PageFragmentation>bucketizer.config[1].config;
        expect(pageConfig.pageSize).toBe(2);
    });

    test("rdfc:Bucketize with RTree strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:RTreeFragmentation;
    tree:latitudePath <latitude>;
    tree:longitudePath <longitude>;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("Bucketize");

        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config.length).toBe(1);
        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#RTreeFragmentation",
        );
        const rtreeConfig = <RTreeFragmentation>bucketizer.config[0].config;
        expect(rtreeConfig.latitudePath).toBeDefined();
        expect(rtreeConfig.longitudePath).toBeDefined();
        expect(rtreeConfig.pageSize).toBe(2);
    });

    test("rdfc:Bucketize with ReversedPage strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:ReversedPageFragmentation;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#ReversedPageFragmentation",
        );
        const config = <PageFragmentation>bucketizer.config[0].config;
        expect(config.pageSize).toBe(2);
    });

    test("rdfc:Bucketize with Timebased strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:TimebasedFragmentation;
    tree:timestampPath <path>;
    tree:maxSize 2;
    tree:k 2;
    tree:minBucketSpan 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#TimebasedFragmentation",
        );
        const config = <TimebasedFragmentation>bucketizer.config[0].config;
        expect(config.path).toBeDefined();
        expect(config.maxSize).toBe(2);
        expect(config.k).toBe(2);
        expect(config.minBucketSpan).toBe(2);
    });

    test("rdfc:Bucketize with TimeBucket strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:TimeBucketFragmentation;
    tree:timestampPath <path>;
    tree:level [
        tree:range "year";
        tree:maxSize 10;
    ];
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#TimeBucketFragmentation",
        );
        const config = <TimeBucketTreeConfig>bucketizer.config[0].config;
        expect(config.path).toBeDefined();
        expect(config.levels.length).toBe(1);
    });

    test("rdfc:Bucketize with Hour strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:HourFragmentation;
    tree:timestampPath <path>;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#HourFragmentation",
        );
        const config = <HourFragmentation>bucketizer.config[0].config;
        expect(config.path).toBeDefined();
    });

    test("rdfc:Bucketize with Dump strategy is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:DumpFragmentation;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:prefix "root".
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.config[0].type.value).toBe(
            "https://w3id.org/tree#DumpFragmentation",
        );
        const config = <DumpFragmentation>bucketizer.config[0].config;
        expect(config).toBeDefined();
    });

    test("rdfc:Ldesify is properly defined", async () => {
        const processor = `
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
        const procHelper = new ProcHelper<FullProc<Ldesify>>();
        // Load processor semantic definition
        await procHelper.importFile(configLocation);
        // Load processor instance declaration
        await procHelper.importInline("pipeline.ttl", processor);

        // Get processor configuration
        procHelper.getConfig("Ldesify");

        // Instantiate processor from declared instance
        const ldesify: FullProc<Ldesify> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(ldesify.reader.uri).toContain("jr");
        expect(ldesify.writer.uri).toContain("jw");
        expect(ldesify.statePath).toBe("save.json");
        expect(ldesify.modifiedPath?.value).toBe("http://example.com/ns#time");
        expect(ldesify.isVersionOfPath?.value).toBe(
            "http://example.com/ns#ver",
        );
        expect(ldesify.checkProps).toBe(true);
    });

    test("js:LdesifySDS is properly defined", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:LdesifySDS;
          rdfc:input <jr>;
          rdfc:statePath "save.json";
          rdfc:output <jw>;
          rdfc:checkProps true;
          rdfc:timestampPath <http://example.com/ns#time>;
          rdfc:versionOfPath <http://example.com/ns#ver>;
          rdfc:sourceStream <http://example.com/ns#source>;
          .
        `;

        const configLocation = process.cwd() + "/configs/ldesify.ttl";
        const procHelper = new ProcHelper<FullProc<LdesifySDS>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("LdesifySDS");

        const ldesifySds: FullProc<LdesifySDS> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(ldesifySds.reader.uri).toContain("jr");
        expect(ldesifySds.writer.uri).toContain("jw");
        expect(ldesifySds.statePath).toBe("save.json");
        expect(ldesifySds.modifiedPathM?.value).toBe(
            "http://example.com/ns#time",
        );
        expect(ldesifySds.isVersionOfPathM?.value).toBe(
            "http://example.com/ns#ver",
        );
        await ldesifySds.init();
        expect(ldesifySds.targetStream.value).toBe(ldesifySds.writer.uri);
        expect(ldesifySds.sourceStream?.value).toBe(
            "http://example.com/ns#source",
        );
    });

    test("generator", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor>  a rdfc:Generate;
          rdfc:count 5;
          rdfc:waitMS 500;
          rdfc:timestampPath <http://out.com#out>;
          rdfc:output <jw>.
        `;

        const configLocation = process.cwd() + "/configs/generator.ttl";
        const procHelper = new ProcHelper<FullProc<Generator>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("Generate");

        const generator: FullProc<Generator> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(generator.writer.uri).toContain("jw");
        expect(generator.wait).toBe(500);
        expect(generator.timestampPath?.value).toBe("http://out.com#out");
        expect(generator.count).toBe(5);
    });

    test("sdsify", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:Sdsify;
            rdfc:input <jr>;
            rdfc:output <jw>;
            rdfc:metadataConfig [
                rdfc:description "My description";
                rdfc:dataset <http://ex.org/dataset>;
                rdfc:timestampPath <http://ex.org/timestamp>;
                rdfc:versionOfPath <http://ex.org/version>;
                rdfc:shapeIri <http://ex.org/shape>;
                rdfc:shape """
                    @prefix sh: <http://www.w3.org/ns/shacl#>.
                    @prefix ex: <http://ex.org/>.
                    
                    [] a sh:NodeShape;
                        sh:targetClass ex:SomeClass;
                        sh:property [
                            sh:path ex:somePath;
                            sh:name "someProperty";
                        ].
                """
            ];
            rdfc:typeFilter <http://ex.org/Type>, <http://ex.org/AnotherType>;
            rdfc:timestampPath <http://ex.org/timestamp>;
            rdfc:shape """
              @prefix sh: <http://www.w3.org/ns/shacl#>.
              @prefix ex: <http://ex.org/>.

              [ ] a sh:NodeShape;
                sh:targetClass ex:SomeClass.
            """;
            rdfc:readAsStream true.
          `;

        const configLocation = process.cwd() + "/configs/sdsify.ttl";
        const procHelper = new ProcHelper<FullProc<Sdsify>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("Sdsify");

        const sdsify: FullProc<Sdsify> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(sdsify.input.uri).toContain("jr");
        expect(sdsify.output.uri).toContain("jw");
        expect(sdsify.metadataConfig?.dataset?.value).toBe(
            "http://ex.org/dataset",
        );
        expect(sdsify.metadataConfig?.timestampPath?.value).toBe(
            "http://ex.org/timestamp",
        );
        expect(sdsify.metadataConfig?.versionOfPath?.value).toBe(
            "http://ex.org/version",
        );
        expect(sdsify.metadataConfig?.shapeIri?.value).toBe(
            "http://ex.org/shape",
        );
        expect(sdsify.metadataConfig?.shape).toBeDefined();
        expect(sdsify.types?.map((x) => x.value)).toEqual([
            "http://ex.org/Type",
            "http://ex.org/AnotherType",
        ]);
        expect(sdsify.readAsStream).toBe(true);
    });

    test("streamJoin", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.
          <http://example.com/ns#processor> a rdfc:StreamJoin;
            rdfc:input <jr>, <jr2>;
            rdfc:output <jw>.
          `;

        const configLocation = process.cwd() + "/configs/stream_join.ttl";
        const procHelper = new ProcHelper<FullProc<StreamJoin>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("StreamJoin");

        const streamJoin: FullProc<StreamJoin> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(streamJoin.inputs.length).toBe(2);
        for (const i of streamJoin.inputs) {
            expect(i.uri).toBeDefined();
        }
        expect(streamJoin.output.uri).toContain("jw");
    });

    test("memberAsNamedGraph", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.
          <http://example.com/ns#processor> a rdfc:MemberAsNamedGraph;
            rdfc:input <jr>;
            rdfc:output <jw>.
          `;

        const configLocation = process.cwd() + "/configs/member_as_graph.ttl";
        const procHelper = new ProcHelper<FullProc<MemberAsNamedGraph>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("MemberAsNamedGraph");

        const memberAsNamedGraph: FullProc<MemberAsNamedGraph> =
            await procHelper.getProcessor("http://example.com/ns#processor");

        expect(memberAsNamedGraph.input.uri).toContain("jr");
        expect(memberAsNamedGraph.output.uri).toContain("jw");
    });

    test("js:LdesDiskWriter is properly defined", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.
            <http://example.com/ns#processor> a rdfc:LdesDiskWriter;
                rdfc:dataInput <jr1>;
                rdfc:directory "/tmp/ldes-disk/".
            `;

        const configLocation = process.cwd() + "/configs/ldes_disk_writer.ttl";
        const procHelper = new ProcHelper<FullProc<LdesDiskWriter>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("LdesDiskWriter");

        const ldesDiskWriter: FullProc<LdesDiskWriter> =
            await procHelper.getProcessor("http://example.com/ns#processor");

        expect(ldesDiskWriter.data.uri).toContain("jr1");
        // It receives the whole pipeline description, its own declaration included
        expect(
            ldesDiskWriter.pipeline.some(
                (q) => q.subject.value === "http://example.com/ns#processor",
            ),
        ).toBe(true);
        expect(ldesDiskWriter.directory).toBe("/tmp/ldes-disk/");
    });

    test("rdfc:Shapify is properly defined", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

            <http://example.com/ns#processor> a rdfc:Shapify;
                rdfc:input <jr>;
                rdfc:output <jw>;
                rdfc:shape <MyShape>.
            `;

        const configLocation = process.cwd() + "/configs/shapify.ttl";
        const procHelper = new ProcHelper<FullProc<Shapify>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("Shapify");

        const shapify: FullProc<Shapify> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(shapify.reader.uri).toContain("jr");
        expect(shapify.writer.uri).toContain("jw");
        expect(shapify.shape).toBeDefined();
        expect(shapify.shape.id.value).toContain("MyShape");
    });

    test("rdfc:Bucketize falls back to its default values", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:dataOutput <dataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:HourFragmentation;
    tree:timestampPath <path>;
  ] [
    a tree:TimeBucketFragmentation;
    tree:timestampPath <path>;
    tree:level [
        tree:range "year";
        tree:maxSize 10;
    ];
  ] ).
    `;

        const configLocation = process.cwd() + "/configs/bucketizer.ttl";
        const procHelper = new ProcHelper<FullProc<Bucketizer>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Bucketize");
        const bucketizer: FullProc<Bucketizer> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(bucketizer.prefix).toBe("root");
        expect(bucketizer.config.length).toBe(2);
        const hour = <HourFragmentation>bucketizer.config[0].config;
        expect(hour.unorderedRelations).toBe(false);
        const timeBucket = <TimeBucketTreeConfig>bucketizer.config[1].config;
        expect(timeBucket.timeBufferMs).toBe(0);
    });

    test("rdfc:Ldesify falls back to its default values", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:Ldesify;
          rdfc:input <jr>;
          rdfc:output <jw>.
        `;

        const configLocation = process.cwd() + "/configs/ldesify.ttl";
        const procHelper = new ProcHelper<FullProc<Ldesify>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Ldesify");
        const ldesify: FullProc<Ldesify> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(ldesify.statePath).toBeUndefined();
        expect(ldesify.checkProps).toBe(true);
        expect(ldesify.modifiedPath.value).toBe(
            "http://purl.org/dc/terms/modified",
        );
        expect(ldesify.isVersionOfPath.value).toBe(
            "http://purl.org/dc/terms/isVersionOf",
        );
    });

    test("js:LdesifySDS falls back to its default values", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:LdesifySDS;
          rdfc:input <jr>;
          rdfc:output <jw>.
        `;

        const configLocation = process.cwd() + "/configs/ldesify.ttl";
        const procHelper = new ProcHelper<FullProc<LdesifySDS>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("LdesifySDS");
        const ldesifySds: FullProc<LdesifySDS> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(ldesifySds.modifiedPathM.value).toBe(
            "http://purl.org/dc/terms/modified",
        );
        expect(ldesifySds.isVersionOfPathM.value).toBe(
            "http://purl.org/dc/terms/isVersionOf",
        );
    });

    test("generator falls back to its default values", async () => {
        const processor = `
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor>  a rdfc:Generate;
          rdfc:output <jw>.
        `;

        const configLocation = process.cwd() + "/configs/generator.ttl";
        const procHelper = new ProcHelper<FullProc<Generator>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("Generate");
        const generator: FullProc<Generator> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(generator.count).toBe(100000);
        expect(generator.wait).toBe(50);
        expect(generator.timestampPath).toBeUndefined();
    });
    test("locations can be written as IRIs", async () => {
        // Absolute IRIs to a temporary folder (with a space), as the
        // processors write their state there on exit
        const dir = join(mkdtempSync(join(tmpdir(), "sds-")), "my state");
        mkdirSync(dir);
        const iri = (name: string) => `<${fromPath(join(dir, name))}>`;

        const bucketizer = await getProc<Bucketizer>(
            "bucketizer.ttl",
            "Bucketize",
            `<http://example.com/ns#processor> a rdfc:Bucketize;
                rdfc:channels [
                    rdfc:dataInput <dataInput>;
                    rdfc:dataOutput <dataOutput>;
                ];
                rdfc:bucketizeStrategy ( [ a tree:DumpFragmentation ] );
                rdfc:savePath ${iri("buckets.json")}.`,
        );
        await bucketizer.init();
        expect(bucketizer.savePath).toBe(join(dir, "buckets.json"));

        // A relative IRI is resolved against the pipeline
        const diskWriter = await getProc<LdesDiskWriter>(
            "ldes_disk_writer.ttl",
            "LdesDiskWriter",
            `<http://example.com/ns#processor> a rdfc:LdesDiskWriter;
                rdfc:dataInput <jr1>;
                rdfc:directory <./ldes-disk/>.`,
        );
        await diskWriter.init();
        expect(diskWriter.directory).toBe(resolve("./ldes-disk") + "/");

        const ldesify = await getProc<Ldesify>(
            "ldesify.ttl",
            "Ldesify",
            `<http://example.com/ns#processor> a rdfc:Ldesify;
                rdfc:input <jr>;
                rdfc:output <jw>;
                rdfc:path ${iri("ldesify.json")}.`,
        );
        await ldesify.init();
        expect(ldesify.statePath).toBe(join(dir, "ldesify.json"));

        // The state is read from the location
        writeFileSync(
            join(dir, "sds.json"),
            JSON.stringify({ "http://ex.org/a": "key" }),
        );
        const ldesifySds = await getProc<LdesifySDS>(
            "ldesify.ttl",
            "LdesifySDS",
            `<http://example.com/ns#processor> a rdfc:LdesifySDS;
                rdfc:input <jr>;
                rdfc:output <jw>;
                rdfc:statePath ${iri("sds.json")}.`,
        );
        await ldesifySds.init();
        expect(ldesifySds.statePath).toBe(join(dir, "sds.json"));
        expect(ldesifySds.cache).toEqual({ "http://ex.org/a": "key" });
    });
});
