import { describe, expect, test } from "@jest/globals";
import { extractProcessors, extractSteps, Source } from "@ajuvercr/js-runner";
import { Parser } from "n3";

describe("SDS processors tests", async () => {
   const pipeline = `
@prefix js: <https://w3id.org/conn/js#>.
@prefix ws: <https://w3id.org/conn/ws#>.
@prefix : <https://w3id.org/conn#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, 
  <./configs/bucketizer.ttl>,
  <./configs/generator.ttl>,
  <./configs/ldesify.ttl>,
  <./configs/sdsify.ttl>,
  <./configs/stream_join.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.

[ ] a :Channel;
  :reader <jr2>.
<jr2> a js:JsReaderChannel.
`;

   const baseIRI = process.cwd() + "/config.ttl";

   test("js:Bucketize is properly defined", async () => {
      const processor = `
    [ ] a js:Bucketize;
      js:dataInput <jr>;
      js:metadataInput <jr>;
      js:dataOutput <jw>;
      js:metadataOutput <jw>;
      js:bucketizeStrategy <./test.js>;
      js:inputStreamId <http://testStream>;
      js:outputStreamId <http://newStream>;
      js:savePath <./save.js>.
    `;

      const source: Source = {
         value: pipeline + processor,
         baseIRI,
         type: "memory",
      };

      const { processors, quads, shapes: config } = await extractProcessors(source);

      const proc = processors[0];
      expect(proc).toBeDefined();

      const argss = extractSteps(proc, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(8);

      const [[i, mi, o, mo, loc, save, si, so]] = argss;
      testReader(i);
      testReader(mi);
      testWriter(o);
      testWriter(mo);

      expect(loc).toBe(process.cwd() + "/test.js");
      expect(si).toBe("http://testStream");
      expect(so).toBe("http://newStream");
      expect(save).toBe(process.cwd() + "/save.js");

      await checkProc(proc.file, proc.func);
   });

   test("js:Ldesify is properly defined", async () => {
      const processor = `
    [ ] a js:Ldesify;
      js:input <jr>;
      js:path "save.json";
      js:output <jw>.
    `;

      const source: Source = {
         value: pipeline + processor,
         baseIRI,
         type: "memory",
      };

      const { processors, quads, shapes: config } = await extractProcessors(source);

      const proc = processors[0];
      expect(proc).toBeDefined();

      const argss = extractSteps(proc, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(3);

      const [[input, output, save]] = argss;
      testReader(input);
      testWriter(output);
      expect(save).toBe("save.json");
      await checkProc(proc.file, proc.func);
   });

   test("generator", async () => {
      const processor = `
    [ ] a js:Generate;
      js:count 5;
      js:waitMS 500;
      js:timestampPath <http://out.com#out>;
      js:output <jw>.
    `;

      const source: Source = {
         value: pipeline + processor,
         baseIRI,
         type: "memory",
      };

      const { processors, quads, shapes: config } = await extractProcessors(source);

      const proc = processors[0];
      expect(proc).toBeDefined();

      const argss = extractSteps(proc, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(4);

      const [[output, count, wait, path]] = argss;
      testWriter(output);
      expect(count).toBe(5);
      expect(wait).toBe(500);
      expect(path.value).toBe("http://out.com#out");

      await checkProc(proc.file, proc.func);
   });

   test("sdsify", async () => {
      const processor = `
    [ ] a js:Sdsify;
      js:input <jr>;
      js:output <jw>;
      js:stream <http://me.com/stream>;
      js:shapeFilter """
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
          sh:targetClass ex:SomeClass.
      """.
    `;

      const source: Source = {
         value: pipeline + processor,
         baseIRI,
         type: "memory",
      };

      const { processors, quads, shapes: config } = await extractProcessors(source);

      const proc = processors[0];
      expect(proc).toBeDefined();

      const argss = extractSteps(proc, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(4);

      const [[input, output, stream, shapeFilters]] = argss;
      testReader(input);
      testWriter(output);
      expect(stream.value).toBe("http://me.com/stream");
      expect(new Parser().parse(shapeFilters[0]).length).toBe(2);

      await checkProc(proc.file, proc.func);
   });

   test("streamJoin", async () => {
      const processor = `
    [ ] a js:StreamJoin;
      js:input <jr>;
      js:input <jr2>;
      js:output <jw>.
    `;

      const source: Source = {
         value: pipeline + processor,
         baseIRI,
         type: "memory",
      };

      const { processors, quads, shapes: config } = await extractProcessors(source);

      const proc = processors[0];
      expect(proc).toBeDefined();

      const argss = extractSteps(proc, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(2);

      const [[inputs, output]] = argss;
      testReader(inputs[0]);
      testReader(inputs[1])
      testWriter(output);

      await checkProc(proc.file, proc.func);
   });
});

function testReader(arg: any) {
   expect(arg).toBeInstanceOf(Object);
   expect(arg.channel).toBeDefined();
   expect(arg.channel.id).toBeDefined();
   expect(arg.ty).toBeDefined();
}

function testWriter(arg: any) {
   expect(arg).toBeInstanceOf(Object);
   expect(arg.channel).toBeDefined();
   expect(arg.channel.id).toBeDefined();
   expect(arg.ty).toBeDefined();
}

async function checkProc(location: string, func: string) {
   const mod = await import("file://" + location);
   expect(mod[func]).toBeDefined();
}
