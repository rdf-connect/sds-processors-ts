import { describe, expect, test } from "vitest";
import { Parser } from "n3";
import { canonize } from "rdf-canonize";
import { MemberAsNamedGraph } from "../lib/memberAsNamedGraph";
import { createWriter, logger, one } from "@rdfc/js-runner/lib/testUtils";
import { FullProc } from "@rdfc/js-runner";

describe("Functional tests for the memberAsNamedGraph function", () => {
    const INPUT = `
<http://ex.org/18953?t=1723532383> <http://purl.org/dc/terms/isVersionOf> <http://ex.org/18953> .
<http://ex.org/18953?t=1723532383> <http://purl.org/dc/terms/modified> "2024-08-13T06:59:43Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex.org/18953> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/ns#MRGeoObject> .
<http://ex.org/18953> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http:/ex.org/ns/placetypes#Bay> .
<http://ex.org/18953> <http://ex.org/ontology#contains> <http://ex.org/30072> .
<http://ex.org/18953> <http://ex.org/ontology#isAdjacentTo> <http://ex.org/19498> .
<http://ex.org/18953> <http://ex.org/ontology#isPartOf> <http://ex.org/14632> .
<http://ex.org/18953> <http://ex.org/ontology#isRelatedTo> <http://ex.org/1912> .
<http://ex.org/18953> <http://ex.org/nsontology#replaces> <http://ex.org/30071> .
<http://ex.org/18953> <http://purl.org/dc/terms/modified> "2024-08-13T06:59:43Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex.org/18953> <http://www.w3.org/2004/02/skos/core#prefLabel> "North Inlet"@en .
<http://ex.org/18953> <http://www.w3.org/ns/dcat#centroid> "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT (-79.15 33.3167)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
<http://ex.org/18953> <http://www.w3.org/ns/prov#hadPrimarySource> _:df_24_24 .
_:df_24_24 <http://www.w3.org/2000/01/rdf-schema#label> "IMIS" .
_:df_8_6 <https://w3id.org/sds#stream> <https://ex.org/feed> <https://w3id.org/sds#DataDescription> .
_:df_8_6 <https://w3id.org/sds#payload> <http://ex.org/18953?t=1723532383> <https://w3id.org/sds#DataDescription> .`;

    const EXPECTED_OUTPUT = `
<https://w3id.org/sds#DataDescription> {
    _:df_8_6 <https://w3id.org/sds#stream> <https://ex.org/feed> .
    _:df_8_6 <https://w3id.org/sds#payload> <http://ex.org/18953?t=1723532383> .
}

<http://ex.org/18953?t=1723532383> <http://purl.org/dc/terms/isVersionOf> <http://ex.org/18953> .
<http://ex.org/18953?t=1723532383> <http://purl.org/dc/terms/modified> "2024-08-13T06:59:43Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .

<http://ex.org/18953?t=1723532383> {
    <http://ex.org/18953> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/ns#MRGeoObject> .
    <http://ex.org/18953> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http:/ex.org/ns/placetypes#Bay> .
    <http://ex.org/18953> <http://ex.org/ontology#contains> <http://ex.org/30072> .
    <http://ex.org/18953> <http://ex.org/ontology#isAdjacentTo> <http://ex.org/19498> .
    <http://ex.org/18953> <http://ex.org/ontology#isPartOf> <http://ex.org/14632> .
    <http://ex.org/18953> <http://ex.org/ontology#isRelatedTo> <http://ex.org/1912> .
    <http://ex.org/18953> <http://ex.org/nsontology#replaces> <http://ex.org/30071> .
    <http://ex.org/18953> <http://purl.org/dc/terms/modified> "2024-08-13T06:59:43Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    <http://ex.org/18953> <http://www.w3.org/2004/02/skos/core#prefLabel> "North Inlet"@en .
    <http://ex.org/18953> <http://www.w3.org/ns/dcat#centroid> "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT (-79.15 33.3167)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
    <http://ex.org/18953> <http://www.w3.org/ns/prov#hadPrimarySource> _:df_24_24 .
    _:df_24_24 <http://www.w3.org/2000/01/rdf-schema#label> "IMIS" .
}
`;

    test("A regular member in a SDS records is transformed into a named graph member", async () => {
        const [inputWriter, inputReader] = createWriter();
        const [outputWriter, outputReader] = createWriter();

        const prom = one(outputReader.strings());
        const proc = <FullProc<MemberAsNamedGraph>>new MemberAsNamedGraph(
            {
                input: inputReader,
                output: outputWriter,
            },
            logger,
        );
        await proc.init();
        const done = proc.transform();
        await inputWriter.string(INPUT);
        await inputWriter.close();
        await done;

        const st = await prom;
        const parser = new Parser();
        const hash = canonize(parser.parse(st!), {
            algorithm: "RDFC-1.0",
        });
        expect(hash).toEqual(
            canonize(parser.parse(EXPECTED_OUTPUT), {
                algorithm: "RDFC-1.0",
            }),
        );
    });
});
