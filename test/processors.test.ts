import { describe, expect, test } from "@jest/globals";
import { extractProcessors, extractSteps, Source } from "@ajuvercr/js-runner";
const prefixes = `
@prefix js: <https://w3id.org/conn/js#>.
@prefix ws: <https://w3id.org/conn/ws#>.
@prefix : <https://w3id.org/conn#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
`;

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

test("bucketstep", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/bucketizer.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
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
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(1);

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

  console.log({ loc: proc.file, func: proc.func });
  await checkProc(proc.file, proc.func);
});

test("ldesify", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/ldesify.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Ldesify;
  js:input <jr>;
  js:path "save.json";
  js:output <jw>.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(1);

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
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/generator.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Generate;
  js:count 5;
  js:waitMS 500;
  js:timestampPath <http://out.com#out>;
  js:output <jw>.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(1);

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

describe("strings/envsub", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/stringManipulations.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.

[ ] a js:Envsub;
  js:input <jr>;
  js:output <jw>.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(2);

  const env = processors.find((x) => x.ty.value.endsWith("Envsub"))!;

  expect(env).toBeDefined();
  const argss = extractSteps(env, quads, config);
  expect(argss.length).toBe(1);
  expect(argss[0].length).toBe(4);

  const [[input, output]] = argss;
  testReader(input);
  testWriter(output);

  await checkProc(env.file, env.func);
});

test("stringManipulations/substitute", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/stringManipulations.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Substitute;
  js:input <jr>;
  js:output <jw>;
  js:source "life";
  js:replace "42";
  js:regexp false.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(2);

  const sub = processors.find((x) => x.ty.value.endsWith("Substitute"))!;

  expect(sub).toBeDefined();
  const argss = extractSteps(sub, quads, config);
  expect(argss.length).toBe(1);
  expect(argss[0].length).toBe(5);

  const [[input, output, s, replace, regexp]] = argss;
  testReader(input);
  testWriter(output);
  expect(s).toBe("life");
  expect(replace).toBe("42");
  expect(regexp).toBe(false);

  await checkProc(sub.file, sub.func);
});

test("sdsify", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/sdsify.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Sdsify;
  js:input <jr>;
  js:output <jw>;
  js:stream <http://me.com/stream>;
  js:objectType <http://myType.com>.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(1);

  const proc = processors[0];
  expect(proc).toBeDefined();

  const argss = extractSteps(proc, quads, config);
  expect(argss.length).toBe(1);
  expect(argss[0].length).toBe(4);

  const [[input, output, stream, ty]] = argss;
  testReader(input);
  testWriter(output);
  expect(stream.value).toBe("http://me.com/stream");
  expect(ty.value).toBe("http://myType.com");

  await checkProc(proc.file, proc.func);
});

test("yarrrml", async () => {
  const value = `${prefixes}
<> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./configs/yarrrml.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Y2R;
  js:input <jr>;
  js:output <jw>.
`;
  const baseIRI = process.cwd() + "/config.ttl";
  console.log(baseIRI);

  const source: Source = {
    value,
    baseIRI,
    type: "memory",
  };

  const { processors, quads, shapes: config } = await extractProcessors(source);
  expect(processors.length).toBe(1);

  const proc = processors[0];
  expect(proc).toBeDefined();

  const argss = extractSteps(proc, quads, config);
  expect(argss.length).toBe(1);
  expect(argss[0].length).toBe(2);

  const [[input, output]] = argss;
  testReader(input);
  testWriter(output);

  await checkProc(proc.file, proc.func);
});
