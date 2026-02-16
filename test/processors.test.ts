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

import type { FullProc } from "@rdfc/js-runner";

describe("SDS processors tests", async () => {
    test("rdfc:Bucketize with complex Subject and Page fragmentation is properly defined", async () => {
        const processor = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix tree: <https://w3id.org/tree#>.

<http://example.com/ns#processor> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:SubjectFragmentation;
    tree:fragmentationPath ( );
  ] [
    a tree:PageFragmentation;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
        expect(bucketizer.channels.metadataInput.uri).toContain(
            "metadataInput",
        );
        expect(bucketizer.channels.dataOutput.uri).toContain("dataOutput");
        expect(bucketizer.channels.metadataOutput.uri).toContain(
            "metadataOutput",
        );
        expect(bucketizer.savePath).toBeUndefined();
        expect(bucketizer.sourceStream?.value).toBe("http://testStream");
        expect(bucketizer.resultingStream?.value).toBe("http://newStream");
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:RTreeFragmentation;
    tree:latitudePath <latitude>;
    tree:longitudePath <longitude>;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:ReversedPageFragmentation;
    tree:pageSize 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:TimebasedFragmentation;
    tree:timestampPath <path>;
    tree:maxSize 2;
    tree:k 2;
    tree:minBucketSpan 2;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
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
    rdfc:outputStreamId <http://newStream>;
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:HourFragmentation;
    tree:timestampPath <path>;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:DumpFragmentation;
  ] );
    rdfc:inputStreamId <http://testStream>;
    rdfc:outputStreamId <http://newStream>;
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
          rdfc:targetStream <http://example.com/ns#target>;
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
        expect(ldesifySds.targetStream.value).toBe(
            "http://example.com/ns#target",
        );
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
        const procHelper = new ProcHelper<FullProc<Sdsify>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);

        procHelper.getConfig("Sdsify");

        const sdsify: FullProc<Sdsify> = await procHelper.getProcessor(
            "http://example.com/ns#processor",
        );

        expect(sdsify.input.uri).toContain("jr");
        expect(sdsify.output.uri).toContain("jw");
        expect(sdsify.streamNode.value).toBe("http://me.com/stream");
        expect(sdsify.timestampPath?.value).toBe("http://ex.org/timestamp");
        expect(sdsify.types?.map((x) => x.value)).toEqual([
            "http://ex.org/Type",
            "http://ex.org/AnotherType",
        ]);
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
                rdfc:metadataInput <jr2>;
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
        expect(ldesDiskWriter.metadata.uri).toContain("jr2");
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
});
