import { describe, expect, test } from "vitest";
import { SimpleStream } from "@rdfc/js-runner";
import { Parser } from "n3";
import { canonize } from "rdf-canonize";
import { memberAsNamedGraph } from "../lib/memberAsNamedGraph";


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
_:df_8_6 <https://w3id.org/sds#payload> <http://ex.org/18953?t=1723532383> <https://w3id.org/sds#DataDescription> .`

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
`

    test("A regular member in a SDS records is transformed into a named graph member", async () => {
        const i = new SimpleStream<string>();
        const o = new SimpleStream<string>();

        
        o.data((data) => {
            const parser = new Parser();
            const hash = canonize(parser.parse(data), { algorithm: "RDFC-1.0" });
            expect(hash).toEqual(canonize(parser.parse(EXPECTED_OUTPUT), { algorithm: "RDFC-1.0" }));
        });

        await memberAsNamedGraph(i, o);
        await i.push(INPUT);
        await i.end();
    });
});