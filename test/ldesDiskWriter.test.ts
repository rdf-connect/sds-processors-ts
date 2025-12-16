import { afterEach, describe, expect, test, vi } from "vitest";
import { LdesDiskWriter } from "../lib/ldesDiskWriter";
import { fs, vol } from "memfs";
import { Parser } from "n3";
import { TREE } from "@treecg/types";
import { createWriter, logger } from "@rdfc/js-runner/lib/testUtils";
import { FullProc } from "@rdfc/js-runner";

vi.mock("fs", async () => {
    const memfs = await vi.importActual("memfs");

    // Support both `import fs from "fs"` and "import { readFileSync } from "fs"`
    return { default: memfs.fs, ...(memfs.fs as object) };
});

describe("Functional tests for the ldesDiskWriter function", () => {
    afterEach(() => {
        vol.reset();
    });

    test("LdesDiskWriter works", async () => {
        const [inputWriter, inputReader] = createWriter();
        const [metaWriter, metaReader] = createWriter();
        const directory = "tmp/ldes-disk/";

        const proc = <FullProc<LdesDiskWriter>>new LdesDiskWriter(
            {
                directory,
                metadata: metaReader,
                data: inputReader,
                nameMap: [],
            },
            logger,
        );
        await proc.init();
        const prom = proc.transform();

        const metadata = `
        @prefix ex: <http://example.org/ns#> .
        @prefix sds: <https://w3id.org/sds#> .
        @prefix dcat: <https://www.w3.org/ns/dcat#> .
        @prefix ldes: <https://w3id.org/ldes#> .
        @prefix prov: <http://www.w3.org/ns/prov#> .
        @prefix dct: <http://purl.org/dc/terms/> .

        ex:BenchmarkStream
            a                     sds:Stream ;
            sds:carries           [ a sds:Member ] ;
            sds:dataset           [ a                  dcat:Dataset ;
                                    dcat:title         "LDES to benchmark" ;
                                    ldes:timestampPath prov:generatedAtTime;
                                    ldes:versionOfPath dct:isVersionOf;
                                    dcat:identifier    <http://localhost:8000/> ] .
        `;

        const data = `
        _:df_9_1 <https://w3id.org/sds#payload> <http://marineregions.org/mrgid/3959?t=1104534000> <https://w3id.org/sds#DataDescription> .
        _:df_9_1 <https://w3id.org/sds#stream> <http://example.org/ns#BenchmarkStream> <https://w3id.org/sds#DataDescription> .
        _:df_9_1 <https://w3id.org/sds#bucket> <root> <https://w3id.org/sds#DataDescription> .
        <http://marineregions.org/mrgid/3959?t=1104534000> <http://purl.org/dc/terms/isVersionOf> <http://marineregions.org/mrgid/3959> .
        <http://marineregions.org/mrgid/3959?t=1104534000> <http://purl.org/dc/terms/modified> "2004-12-31T23:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marineregions.org/ns/ontology#MRGeoObject> .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marineregions.org/ns/placetypes#Canyons> .
        <http://marineregions.org/mrgid/3959> <http://marineregions.org/ns/ontology#isPartOf> <http://marineregions.org/mrgid/4279> .
        <http://marineregions.org/mrgid/3959> <http://purl.org/dc/terms/modified> "2004-12-31T23:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/2004/02/skos/core#exactMatch> <urn:acuf:ufi:45863> .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/2004/02/skos/core#note> ": Named after Skikda city"@en .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/2004/02/skos/core#note> ": Removed from GEBCO gazetteer 2010"@en .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/2004/02/skos/core#note> "Coordinates in ACUF: Latitude: 37.133333; Longitude: 6.783333"@en .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/2004/02/skos/core#prefLabel> "Skikda Canyons"@en .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/ns/dcat#centroid> "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT (6.78333333 37.13333333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
        <http://marineregions.org/mrgid/3959> <http://www.w3.org/ns/prov#hadPrimarySource> <http://www.ngdc.noaa.gov/gazetteer/> .
        <urn:acuf:ufi:45863> <https://schema.org/identifier> _:b5_n3-69958 .
        <urn:acuf:ufi:45863> <https://schema.org/url> <https://geonames.nga.mil/arcgis/rest/services/Research/GeoNames/MapServer/0/query?where=UNIQUE_FEATURE_IDENTIFIER_UFI%3D45863> .
        _:b5_n3-69958 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/PropertyValue> .
        _:b5_n3-69958 <https://schema.org/propertyID> <http://www.wikidata.org/entity/P2326> .
        _:b5_n3-69958 <https://schema.org/value> "45863" .
        <root> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/sds#Bucket> <https://w3id.org/sds#DataDescription> .
        <root> <https://w3id.org/sds#immutable> "false" <https://w3id.org/sds#DataDescription> .
        <root> <https://w3id.org/sds#isRoot> "true" <https://w3id.org/sds#DataDescription> .
        <root> <https://w3id.org/sds#stream> <http://example.org/ns#BenchmarkStream> <https://w3id.org/sds#DataDescription> .
        `;

        await metaWriter.string(metadata);
        await inputWriter.string(data);

        await metaWriter.close();
        await inputWriter.close();
        await prom;

        const files = fs.readdirSync(directory);
        expect(files).toContain("index.trig");
        expect(files).toContain(
            "http_3A_2F_2Fexample.org_2Fns_23BenchmarkStream",
        );

        const streamFiles = fs.readdirSync(
            `${directory}http_3A_2F_2Fexample.org_2Fns_23BenchmarkStream`,
        );
        expect(streamFiles).toContain("index.trig");
        expect(streamFiles).toContain("root");

        const rootFiles = fs.readdirSync(
            `${directory}http_3A_2F_2Fexample.org_2Fns_23BenchmarkStream/root`,
        );
        expect(rootFiles).toContain("index.trig");

        const index = fs.readFileSync(
            `${directory}http_3A_2F_2Fexample.org_2Fns_23BenchmarkStream/root/index.trig`,
        );
        const quads = new Parser({
            baseIRI:
                "http://localhost/http_3A_2F_2Fexample.org_2Fns_23BenchmarkStream/root/index.trig",
        }).parse(index.toString());
        // Check if it contains the tree:member triple
        expect(
            quads.some(
                (q) =>
                    q.subject.value === "http://localhost/index.trig" &&
                    q.predicate.value === TREE.member &&
                    q.object.value ===
                        "http://marineregions.org/mrgid/3959?t=1104534000",
            ),
        ).toBeTruthy();

        // Check if it contains the member triples
        expect(
            quads.filter(
                (q) =>
                    q.subject.value === "http://marineregions.org/mrgid/3959",
            ).length,
        ).toBe(11);
    });
});
