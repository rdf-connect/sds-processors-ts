import { describe, expect, test } from "vitest";
import { DataFactory, Parser } from "n3";
import { extractShapes } from "rdf-lens";
import {
    BucketizerConfig,
    BucketizerOrchestrator,
    RTreeFragmentation,
    SHAPES_TEXT,
} from "../lib/bucketizers/index";
import { Bucket, Record } from "../lib/";
import { BucketRelation, GEO } from "../lib/utils";
import { XSD } from "@treecg/types";

import type { Literal } from "@rdfjs/types";
const { namedNode, literal, quad } = DataFactory;

describe("RTree Bucketizer", () => {
    const quads = new Parser({ baseIRI: "" }).parse(SHAPES_TEXT);
    const shapes = extractShapes(quads);
    const lens = shapes.lenses["https://w3id.org/tree#FragmentationStrategy"];

    const BUCKET_ID_REGEX = /-?[0-9.]+_-?[0-9.]+__-?[0-9.]+_-?[0-9.]+/;

    test("Config parsing", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix ex: <http://example.org/>.
@prefix sds: <https://w3id.org/sds#>.

<a> a tree:RTreeFragmentation;
  tree:wktPath <location>;
  tree:pageSize 2.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        expect(output.type.value).toBe(
            "https://w3id.org/tree#RTreeFragmentation",
        );
        const config = <RTreeFragmentation>output.config;
        expect(config.wktPath).toBeDefined();
        expect(config.pageSize).toBe(2);
    });

    test("Basic partitioning (no split)", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
<a> a tree:RTreeFragmentation;
  tree:wktPath <location>;
  tree:pageSize 5.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];
        const removeRelations: {
            origin: Bucket;
            relation: BucketRelation;
        }[] = [];

        const record = (id: string, wkt: string) =>
            new Record(
                {
                    id: namedNode(id),
                    quads: [
                        quad(
                            namedNode(id),
                            namedNode("location"),
                            literal(wkt, GEO.wktLiteral),
                        ),
                    ],
                },
                stream,
            );

        const r1 = record("a1", "POINT(1 1)");
        const results = orchestrator.bucketize(
            r1,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        expect(results).toEqual([""]);
        expect(buckets[""].root).toBeTruthy();
    });

    test("Split logic", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
<a> a tree:RTreeFragmentation;
  tree:wktPath <location>;
  tree:pageSize 2.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const removeRelations: { origin: Bucket; relation: BucketRelation }[] =
            [];

        const record = (id: string, wkt: string) =>
            new Record(
                {
                    id: namedNode(id),
                    quads: [
                        quad(
                            namedNode(id),
                            namedNode("location"),
                            literal(wkt, GEO.wktLiteral),
                        ),
                    ],
                },
                stream,
            );

        // First 2 records fill the root (which is leaf)
        expect(
            orchestrator.bucketize(
                record("a1", "POINT(0 0)"),
                buckets,
                requestedBuckets,
                newMembers,
                newRelations,
                removeRelations,
                "",
            ),
        ).toEqual([""]);
        expect(
            orchestrator.bucketize(
                record("a2", "POINT(1 1)"),
                buckets,
                requestedBuckets,
                newMembers,
                newRelations,
                removeRelations,
                "",
            ),
        ).toEqual([""]);

        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(0);

        // 3rd record triggers split
        const results = orchestrator.bucketize(
            record("a3", "POINT(10 10)"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        // Members should now be in newly created buckets (leafs)
        expect(results[0]).toMatch(BUCKET_ID_REGEX);

        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);

        const rel1 = buckets[""].links[0];
        expect(rel1.type.value).toBe(
            "https://w3id.org/tree#GeospatiallyContainsRelation",
        );
        expect(rel1.value?.value).toContain("POLYGON");
    });

    test("Persistence", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
<a> a tree:RTreeFragmentation;
  tree:wktPath <location>;
  tree:pageSize 2.
`;
        const quads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");
        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const removeRelations: { origin: Bucket; relation: BucketRelation }[] =
            [];

        const record = (id: string, wkt: string) =>
            new Record(
                {
                    id: namedNode(id),
                    quads: [
                        quad(
                            namedNode(id),
                            namedNode("location"),
                            literal(wkt),
                        ),
                    ],
                },
                stream,
            );

        orchestrator.bucketize(
            record("a1", "POINT(0 0)"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        const state = orchestrator.save();
        expect(JSON.parse(state)[""]).toBeDefined();

        // Load state
        const orchestrator2 = new BucketizerOrchestrator([output], state);
        const results = orchestrator2.bucketize(
            record("a2", "POINT(1 1)"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        expect(results).toEqual([""]);
    });

    test("Separate latitude and longitude properties", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix dwc: <http://rs.tdwg.org/dwc/terms/>.
<a> a tree:RTreeFragmentation;
  tree:latitudePath <latitude>;
  tree:longitudePath <longitude>;
  tree:pageSize 2.
`;
        const configQuads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads: configQuads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const removeRelations: { origin: Bucket; relation: BucketRelation }[] =
            [];

        const record = (id: string, lat: string, lon: string) =>
            new Record(
                {
                    id: namedNode(id),
                    quads: [
                        quad(
                            namedNode(id),
                            namedNode("latitude"),
                            literal(
                                lat,
                                namedNode(
                                    "http://www.w3.org/2001/XMLSchema#double",
                                ),
                            ),
                        ),
                        quad(
                            namedNode(id),
                            namedNode("longitude"),
                            literal(
                                lon,
                                namedNode(
                                    "http://www.w3.org/2001/XMLSchema#double",
                                ),
                            ),
                        ),
                    ],
                },
                stream,
            );

        orchestrator.bucketize(
            record("a1", "42.033333333333", "3.2166666666667"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        orchestrator.bucketize(
            record("a2", "47.16667", "-18.00056"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        const results = orchestrator.bucketize(
            record("a3", "43.52", "7.13"),
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(8);
        expect(results.length).toBe(1);
        expect(results[0]).toMatch(BUCKET_ID_REGEX);
    });

    test("Realistic test with EuroBIS data based on WKT property", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix mr: <http://marineregions.org/ns/ontology#>.
@prefix geo: <http://www.opengis.net/ont/geosparql#>.
<a> a tree:RTreeFragmentation;
  tree:wktPath ( mr:hasGeometry geo:asWKT );
  tree:pageSize 2.
`;
        const configQuads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads: configQuads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const removeRelations: { origin: Bucket; relation: BucketRelation }[] =
            [];

        const prefixes = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix dwc: <http://rs.tdwg.org/dwc/terms/> .
@prefix mr: <http://marineregions.org/ns/ontology#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix sf: <http://www.opengis.net/ont/sf#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
`;

        const entity1 =
            prefixes +
            `
<https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688>
  a mr:Feature , dct:Location ;
  geo:hasCentroid <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688> ;
  mr:hasGeometry <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688> ;
  dcat:centroid "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (42.033333333333 3.2166666666667)"^^geo:wktLiteral ;
  dwc:decimalLatitude "42.033333333333"^^xsd:decimal ;
  dwc:decimalLongitude "3.2166666666667"^^xsd:decimal .

<https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688>
  a sf:Point , sf:Geometry ;
  geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (42.033333333333 3.2166666666667)"^^geo:wktLiteral .
`;

        const entity2 =
            prefixes +
            `
<https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724>
  a mr:Feature , dct:Location ;
  geo:hasCentroid <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724> ;
  mr:hasGeometry <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724> ;
  dcat:centroid "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (47.16667 -18.00056)"^^geo:wktLiteral ;
  dwc:decimalLatitude "47.16667"^^xsd:decimal ;
  dwc:decimalLongitude "-18.00056"^^xsd:decimal .

<https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724>
  a sf:Point , sf:Geometry ;
  geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (47.16667 -18.00056)"^^geo:wktLiteral .
`;

        const entity3 =
            prefixes +
            `
<https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567>
  a mr:Feature , dct:Location ;
  geo:hasCentroid <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567> ;
  mr:hasGeometry <https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567> ;
  dcat:centroid "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (43.52 7.13)"^^geo:wktLiteral ;
  dwc:decimalLatitude "43.52"^^xsd:decimal ;
  dwc:decimalLongitude "7.13"^^xsd:decimal .

<https://eurobis.org/id/point/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567>
  a sf:Point , sf:Geometry ;
  geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (43.52 7.13)"^^geo:wktLiteral .
`;

        const parseRecord = (turtle: string, id: string) => {
            const quads = new Parser().parse(turtle);
            return new Record(
                {
                    id: namedNode(id),
                    quads,
                },
                stream,
            );
        };

        const r1 = parseRecord(
            entity1,
            "https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688",
        );
        const r2 = parseRecord(
            entity2,
            "https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724",
        );
        const r3 = parseRecord(
            entity3,
            "https://eurobis.org/id/location/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567",
        );

        orchestrator.bucketize(
            r1,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        orchestrator.bucketize(
            r2,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        const results = orchestrator.bucketize(
            r3,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(2);
        expect(results.length).toBe(1);
        expect(results[0]).toMatch(BUCKET_ID_REGEX);
        expect(buckets[""].links[0].value?.value).toContain(
            "<http://www.opengis.net/def/crs/EPSG/0/4326>",
        );
        expect((buckets[""].links[0].value as Literal)?.datatype.value).toBe(
            "http://www.opengis.net/ont/geosparql#wktLiteral",
        );
    });

    test("Realistic test with EuroBIS data based on lat/long properties", () => {
        const quadsStr = `
@prefix tree: <https://w3id.org/tree#>.
@prefix mr: <http://marineregions.org/ns/ontology#>.
@prefix geo: <http://www.opengis.net/ont/geosparql#>.
@prefix dwc: <http://rs.tdwg.org/dwc/terms/>.
<a> a tree:RTreeFragmentation;
  tree:latitudePath dwc:latitude;
  tree:longitudePath dwc:longitude;
  tree:pageSize 2.
`;
        const configQuads = new Parser({ baseIRI: "" }).parse(quadsStr);
        const output = <BucketizerConfig>lens.execute({
            id: namedNode("a"),
            quads: configQuads,
        });

        const orchestrator = new BucketizerOrchestrator([output]);
        const stream = namedNode("MyStream");

        const buckets: { [id: string]: Bucket } = {};
        const requestedBuckets = new Set<string>();
        const newMembers = new Map<string, Set<string>>();
        const newRelations: { origin: Bucket; relation: BucketRelation }[] = [];
        const removeRelations: { origin: Bucket; relation: BucketRelation }[] =
            [];

        const prefixes = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix dwc: <http://rs.tdwg.org/dwc/terms/> .
@prefix mr: <http://marineregions.org/ns/ontology#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix sf: <http://www.opengis.net/ont/sf#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix marine-observation: <https://eurobis.org/ns/marine-observation/> .
@prefix dwciri: <http://rs.tdwg.org/dwc/iri/> .
`;

        const entity1 =
            prefixes +
            `
<https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688>
  a marine-observation:OrganismOccurrence , dwc:Occurrence ;
  dct:identifier "WoRMS Editorial Board:WoRMS type localities:dr_id_314688" ;
  marine-observation:madeDuringEvent <https://eurobis.org/id/event/5184_> ;
  dwc:basisOfRecord "PreservedSpecimen" ;
  marine-observation:hasOccurrenceBasis <http://rs.tdwg.org/dwc/dwctype/PreservedSpecimen> ;
  dwc:fieldNumber "" ;
  marine-observation:hasOccurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  dwciri:occurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  marine-observation:isOfOrganismNamedAs <https://aphia.org/id/taxname/> ;
  dwc:dateLastModified '2018-08-04'^^xsd:date;
  dwc:institutionCode 'WoRMS Editorial Board'^^xsd:string;
  dwc:collectionCode 'WoRMS type localities'^^xsd:string;
  dwc:catalogNumber 'dr_id_314688'^^xsd:string;
  dwc:recordUrl 'http://www.marinespecies.org/aphia.php?p=distribution&id=314688'^^xsd:anyURI;
  dwc:scientificName 'Clathrina hispanica'^^xsd:string;
  dwc:basisOfRecord 'PreservedSpecimen'^^xsd:string;
  dwc:typeStatus 'holotype'^^xsd:string;
  dwc:longitude '3.2166666666667'^^xsd:double;
  dwc:latitude '42.033333333333'^^xsd:double;
  dwc:occurrenceRemarks 'common'^^xsd:string;
  dwc:aphiaid '150259'^^xsd:integer;
  dwc:occurrenceId 'WoRMS Editorial Board:WoRMS type localities:dr_id_314688'^^xsd:string;
  dwc:aphiaIdAccepted '150259'^^xsd:integer;
  dwc:taxonRank '220'^^xsd:integer.
`;

        const entity2 =
            prefixes +
            `
<https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724>
  a marine-observation:OrganismOccurrence , dwc:Occurrence ;
  dct:identifier "WoRMS Editorial Board:WoRMS type localities:dr_id_315724" ;
  marine-observation:madeDuringEvent <https://eurobis.org/id/event/5184_> ;
  dwc:basisOfRecord "PreservedSpecimen" ;
  marine-observation:hasOccurrenceBasis <http://rs.tdwg.org/dwc/dwctype/PreservedSpecimen> ;
  dwc:fieldNumber "" ;
  marine-observation:hasOccurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  dwciri:occurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  marine-observation:isOfOrganismNamedAs <https://aphia.org/id/taxname/> ;
  dwc:dateLastModified '2014-05-07'^^xsd:date;
  dwc:institutionCode 'WoRMS Editorial Board'^^xsd:string;
  dwc:collectionCode 'WoRMS type localities'^^xsd:string;
  dwc:catalogNumber 'dr_id_315724'^^xsd:string;
  dwc:recordUrl 'http://www.marinespecies.org/aphia.php?p=distribution&id=315724'^^xsd:anyURI;
  dwc:scientificName 'Anthoactis benedeni'^^xsd:string;
  dwc:basisOfRecord 'PreservedSpecimen'^^xsd:string;
  dwc:typeStatus 'holotype'^^xsd:string;
  dwc:yearCollected '1922'^^xsd:integer;
  dwc:startYearCollected '1922'^^xsd:integer;
  dwc:monthCollected '7'^^xsd:integer;
  dwc:startMonthCollected '7'^^xsd:integer;
  dwc:dayCollected '26'^^xsd:integer;
  dwc:startDayCollected '26'^^xsd:integer;
  dwc:longitude '-18.00056'^^xsd:double;
  dwc:latitude '47.16667'^^xsd:double;
  dwc:minimumDepth '2250'^^xsd:integer;
  dwc:occurrenceRemarks "'Armauer Hansen' St. 43"^^xsd:string;
  dwc:aphiaid '151566'^^xsd:integer;
  dwc:occurrenceId 'WoRMS Editorial Board:WoRMS type localities:dr_id_315724'^^xsd:string;
  dwc:observationDate '1922-07-26'^^xsd:date;
  dwc:aphiaIdAccepted '151566'^^xsd:integer;
  dwc:timePrecision '100'^^xsd:integer;
  dwc:taxonRank '220'^^xsd:integer.
`;

        const entity3 =
            prefixes +
            `
<https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567>
  a marine-observation:OrganismOccurrence , dwc:Occurrence ;
  dct:identifier "WoRMS Editorial Board:WoRMS type localities:dr_id_394567" ;
  marine-observation:madeDuringEvent <https://eurobis.org/id/event/5184_> ;
  dwc:basisOfRecord "PreservedSpecimen" ;
  marine-observation:hasOccurrenceBasis <http://rs.tdwg.org/dwc/dwctype/PreservedSpecimen> ;
  dwc:fieldNumber "" ;
  marine-observation:hasOccurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  dwciri:occurrenceStatus <https://eurobis.org/ns/occurrence-status#present> ;
  marine-observation:isOfOrganismNamedAs <https://aphia.org/id/taxname/> ;
  dwc:dateLastModified '2016-06-09'^^xsd:date;
  dwc:institutionCode 'WoRMS Editorial Board'^^xsd:string;
  dwc:collectionCode 'WoRMS type localities'^^xsd:string;
  dwc:catalogNumber 'dr_id_394567'^^xsd:string;
  dwc:recordUrl 'http://www.marinespecies.org/aphia.php?p=distribution&id=394567'^^xsd:anyURI;
  dwc:scientificName 'Kraspedonema octogoniatum'^^xsd:string;
  dwc:basisOfRecord 'PreservedSpecimen'^^xsd:string;
  dwc:typeStatus 'holotype'^^xsd:string;
  dwc:longitude '7.13'^^xsd:double;
  dwc:latitude '43.52'^^xsd:double;
  dwc:minimumDepth '0'^^xsd:integer;
  dwc:maximumDepth '0'^^xsd:integer;
  dwc:aphiaid '120848'^^xsd:integer;
  dwc:occurrenceId 'WoRMS Editorial Board:WoRMS type localities:dr_id_394567'^^xsd:string;
  dwc:aphiaIdAccepted '230077'^^xsd:integer;
  dwc:taxonRank '220'^^xsd:integer.
`;

        const parseRecord = (turtle: string, id: string) => {
            const quads = new Parser().parse(turtle);
            return new Record(
                {
                    id: namedNode(id),
                    quads,
                },
                stream,
            );
        };

        const r1 = parseRecord(
            entity1,
            "https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_314688",
        );
        const r2 = parseRecord(
            entity2,
            "https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_315724",
        );
        const r3 = parseRecord(
            entity3,
            "https://eurobis.org/id/occurrence/5184__WoRMS%20Editorial%20Board%3AWoRMS%20type%20localities%3Adr_id_394567",
        );

        orchestrator.bucketize(
            r1,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        orchestrator.bucketize(
            r2,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );
        const results = orchestrator.bucketize(
            r3,
            buckets,
            requestedBuckets,
            newMembers,
            newRelations,
            removeRelations,
            "",
        );

        expect(buckets[""].root).toBeTruthy();
        expect(buckets[""].links.length).toBe(8);
        expect(results.length).toBe(1);
        expect(results[0]).toMatch(BUCKET_ID_REGEX);
        expect(parseFloat(buckets[""].links[0].value!.value)).toBeDefined();
        expect((buckets[""].links[0].value as Literal)?.datatype.value).toBe(
            XSD.custom("double"),
        );
    });
});
