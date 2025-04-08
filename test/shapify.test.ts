import { describe, expect, test } from "vitest";
import { Shapify } from "../lib/shapify.js";
import { NamedNode, Parser } from "n3";
import { CBDShapeExtractor, Extractor } from "../lib";
import { createWriter, logger, one } from "@rdfc/js-runner/lib/testUtils.js";
import { FullProc } from "@rdfc/js-runner";

describe("Functional tests for the shapify function", () => {
    test("Shapify works as expected for mumo", async () => {
        const shape = `
@prefix dcterms: <http://purl.org/dc/terms/>.
@prefix sosa: <http://www.w3.org/ns/sosa/>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix js: <https://w3id.org/conn/js#>.
@prefix fno: <https://w3id.org/function/ontology#>.
@prefix fnom: <https://w3id.org/function/vocabulary/mapping#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix : <https://w3id.org/conn#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix dc: <http://purl.org/dc/terms/>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.

<NodeShape> a sh:NodeShape;
  sh:closed true;
  sh:property [
    sh:path <http://www.cidoc-crm.org/cidoc-crm/P55_has_current_location>;
    sh:node [
      sh:closed true;
      sh:property [ sh:path [ sh:zeroOrMorePath dcterms:isPartOf ] ];
    ];
  ], [ sh:path sosa:hosts ].

<ValueShape> a sh:NodeShape;
  sh:closed true;
  sh:property [ sh:path rdf:type ],
    [ sh:path <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.resultTime> ],
    [
      sh:path <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.result>;
      sh:node [
        sh:closed true;
        sh:property [ sh:path rdf:type ],
          [ sh:path <http://qudt.org/1.1/schema/qudt#unit> ],
          [ sh:path <http://qudt.org/1.1/schema/qudt#Valuet> ];
      ];
    ],
    [
      sh:path sosa:madeBySensor;
      sh:node [
        sh:closed true;
        sh:property [
          sh:path [ sh:inversePath sosa:hosts ];
          sh:node <NodeShape>;
        ];
      ];
    ].`;

        const data = `
@prefix sds: <https://w3id.org/sds#>.
<http://mumo.be/data/1269351363/mumo-v2-011-base-battery> a <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation>;
    <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.resultTime> "1970-01-15T16:35:51.363Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
    <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.result> _:b106_b203_b202_b201_n3-137;
    <http://www.w3.org/ns/sosa/madeBySensor> <https://heron.libis.be/momu-test/api/items/36726>.
_:b106_b203_b202_b201_n3-137 a <http://qudt.org/1.1/schema/qudt#QuantityValue>;
    <http://qudt.org/1.1/schema/qudt#unit> <http://qudt.org/1.1/vocab/unit#Battery>;
    <http://qudt.org/1.1/schema/qudt#numericValue> "3.2795000076293945"^^<http://www.w3.org/2001/XMLSchema#float>.
<https://heron.libis.be/momu-test/api/items/36726> <http://purl.org/dc/terms/identifier> <http://data.momu.be/items/id/70B3D57ED0062094-base-battery>;
    <http://purl.org/dc/terms/isPartOf> <https://heron.libis.be/momu-test/api/items/36720>;
    <http://purl.org/dc/terms/title> "mumo-v2-011 - base - battery";
    a <http://omeka.org/s/vocabs/o#Item>, <http://www.w3.org/ns/sosa/Sensor>.
<https://heron.libis.be/momu-test/api/items/36717> <http://www.w3.org/ns/sosa/hosts> <https://heron.libis.be/momu-test/api/items/36726>, <https://heron.libis.be/momu-test/api/items/36729>, <https://heron.libis.be/momu-test/api/items/36732>;
    <http://purl.org/dc/terms/created> "2024-07-04T18:01:06.422Z";
    <http://purl.org/dc/terms/hasPart> <https://heron.libis.be/momu-test/api/items/36720>;
    <http://purl.org/dc/terms/identifier> <http://data.momu.be/items/id/70B3D57ED0062094>;
    <http://purl.org/dc/terms/title> "mumo-v2-011";
    <http://www.cidoc-crm.org/cidoc-crm/P55_has_current_location> <https://heron.libis.be/momu-test/api/items/20337>;
    <http://www.loc.gov/mods/rdf/v1#identifier> "70B3D57ED0062094";
    a <http://omeka.org/s/vocabs/o#Item>, <http://www.w3.org/ns/sosa/Platform>.
<https://heron.libis.be/momu-test/api/items/36729> <http://purl.org/dc/terms/identifier> <http://data.momu.be/items/id/70B3D57ED0062094-base-pressure>;
    <http://purl.org/dc/terms/isPartOf> <https://heron.libis.be/momu-test/api/items/36720>;
    <http://purl.org/dc/terms/title> "mumo-v2-011 - base - pressure";
    a <http://omeka.org/s/vocabs/o#Item>, <http://www.w3.org/ns/sosa/Sensor>.
<https://heron.libis.be/momu-test/api/items/36732> <http://purl.org/dc/terms/identifier> <http://data.momu.be/items/id/70B3D57ED0062094-base-temperature>;
    <http://purl.org/dc/terms/isPartOf> <https://heron.libis.be/momu-test/api/items/36720>;
    <http://purl.org/dc/terms/title> "mumo-v2-011 - base - temperature";
    a <http://omeka.org/s/vocabs/o#Item>, <http://www.w3.org/ns/sosa/Sensor>.
<https://heron.libis.be/momu-test/api/items/20337> <http://purl.org/dc/terms/title> "SomeEpicTitle".

sds:DataDescription {
[] a sds:Record;
  sds:payload <http://mumo.be/data/1269351363/mumo-v2-011-base-battery>;
sds:stream <blabla>.
}
`;
        const shapeQuads = new Parser().parse(shape);
        const [inputWriter, inputReader] = createWriter();
        const [outputWriter, outputReader] = createWriter();

        const proc = <FullProc<Shapify>>new Shapify(
            {
                shape: {
                    id: new NamedNode("ValueShape"),
                    quads: shapeQuads,
                },
                writer: outputWriter,
                reader: inputReader,
            },
            logger,
        );
        await proc.init();
        proc.transform();

        const prom = one(outputReader.strings());
        await inputWriter.string(data);
        const st = await prom;
        expect(st).toBeDefined();

        const extractor = new Extractor(new CBDShapeExtractor());
        const records = await extractor.parse_records(new Parser().parse(st!));

        expect(records.length, "parsed records is one").toBe(1);
        expect(records[0].data.quads.length).toBe(10);
    });
});
