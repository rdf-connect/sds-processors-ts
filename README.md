# sds-processors

[![Node CI](https://github.com/rdf-connect/sds-processors-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sds-processors-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sds-processors-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sds-processors-ts)

Collection of [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) Typescript processors for handling [SDS (Smart Data Streams)](https://treecg.github.io/SmartDataStreams-Spec/)-related operations. It currently exposes 9 functions:

### [`rdfc:Sdsify`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/sdsify.ttl#L10)

This processor takes as input a stream of (batched) RDF data entities and wraps them as individual SDS records to be further processed downstream. By default, it will extract individual entities by taking every single named node subject and extracting a [Concise Bounded Description](https://www.w3.org/Submission/CBD/) (CBD) of that entity with respect to the input RDF graph.

Alternatively, a set of types may be specified (`rdfc:typeFilter`) to target concrete entities. A SHACL shape can be given to concretely define the bounds target entities and their properties, that want to be extracted and packaged as SDS records. This processor relies on the [member extraction algorithm](https://github.com/TREEcg/extract-cbd-shape) implemented by the [W3C TREE Hypermedia community group](https://www.w3.org/community/treecg/).

If the `rdfc:timestampPath` is specified, the set of SDS records will be streamed out in temporal order to avoid out of order writing issues downstream.

An example of how to use this processor within a RDF-Connect pipeline definition is shown next:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

[ ] a rdfc:Sdsify;
    rdfc:input <inputChannelReader>;
    rdfc:output <outputChannerWriter>;
    rdfc:stream <http://ex.org/myStream>;
    rdfc:typeFilter ex:SomeClass, ex:SomeOtherClass;
    rdfc:timestampPath <http://ex.org/timestamp>;
    rdfc:shape """
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:xone (<shape1> <shape2>).

        <shape1> a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:property [ sh:path ex:someProperty ].

        <shape2> a sh:NodeShape;
            sh:targetClass ex:SomeOtherClass;
            sh:property [
                sh:path ex:someProperty
            ], [
                sh:path ex:someOtherProperty;
                sh:node [
                    a sh:NodeShape;
                    sh:targetClass ex:YetAnotherClass
                ]
            ].
    """.
```

### [`rdfc:Bucketize`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/bucketizer.ttl#L10)

This processor takes as input a stream of SDS records and SDS metadata and proceeds to _bucketize_ them according to a predefined strategy ([see example](https://github.com/rdf-connect/sds-processors-ts/blob/master/bucketizeStrategy.ttl)). The SDS metadata will be also transformed to reflect this transformation. Multiple SDS streams can be present on the incoming data channel.

You can define bucketizers as follows:

#### Example of a subject and page fragmentation

```turtle
<bucketize> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <...data input>;
    rdfc:metadataInput <... metadata input>;
    rdfc:dataOutput <... data output>;
    rdfc:metadataOutput <... metadata output>;
  ];
  rdfc:bucketizeStrategy ( [            # One or more bucketize strategies
    a tree:SubjectFragmentation;      # Create a bucket based on this path
    tree:fragmentationPath ( );
  ] [
    a tree:PageFragmentation;         # Create a new bucket when the previous bucket has 2 members
    tree:pageSize 2;
  ] );
  rdfc:savePath <./buckets_save.json>;
  rdfc:outputStreamId <MyEpicStream>;
  rdfc:prefix "root/".                  # The root fragment is located at '/root/' this defaults to ''
```

#### Example of a time-based fragmentation

```turtle
<bucketize> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <...data input>;
    rdfc:metadataInput <... metadata input>;
    rdfc:dataOutput <... data output>;
    rdfc:metadataOutput <... metadata output>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:TimebasedFragmentation;
    tree:timestampPath <https://www.w3.org/ns/activitystreams#published>;
    tree:maxSize 100;
    tree:k 4;
    tree:minBucketSpan 3600;        # In seconds
  ]);
  rdfc:savePath <./buckets_save.json>;
  rdfc:outputStreamId <MyEpicStream>;
  rdfc:prefix "root/".                  # The root fragment is located at '/root/' this defaults to ''
```

This will create buckets based on a time-based fragmentation.
The `tree:timestampPath` specifies the path to the timestamp property in the SDS records.
The `tree:maxSize` specifies the maximum size of a bucket.
When the bucket reaches the maximum size, it will be split into `tree:k` new buckets, each with 1/k of the original bucket's timespan.
The members will be redistributed to the new buckets based on their timestamps.
The `tree:minBucketSpan` specifies the minimum timespan of a bucket.
If a bucket is full, but splitting the bucket would result in a bucket with a timespan smaller than `tree:minBucketSpan`, the bucket will not be split, but a relation will be added to a new page bucket with same timespan as the full bucket, similar to the page fragmentation.

The members need to be arrived in order of their timestamps.
When a member arrives, all buckets that hold members with a timestamp older than the new member's timestamp will be made immutable and no new members can be added to them.

#### Example of a timebucket based fragmentation

```turtle
<timebucket-fragmentation> a tree:TimeBucketFragmentation;
  tree:timestampPath <http://def.isotc211.org/iso19156/2011/Observation#OM_Observation.resultTime>;
  tree:buffer 5000;   # members can arrive 5 seconds out of sync ()
  tree:level ( [      # Create 5 levels, resulting uri's <year>/<month>/<day>/<hour>/<minute>
    tree:range "year", "month";
    tree:maxSize 0;   # place no members at this level
  ] [
    tree:range "day-of-month";
    tree:maxSize 1000;    # place at most 1000 members at this level
  ] [
    tree:range "hour";
    tree:maxSize 1000;    # place at most 1000 members at this level
  ] [
    tree:range "minute";
    tree:maxSize 10000;   # place at most 10000 members at this level, this is the last level thus excess members are also put in this level
  ] ).
```

This fragmentation will look like this `${year}-${month}/${day}/${hour}/${minute}` after ingesting 2001 members in the same hour (filling day and hour).

#### Example of a spatial (R-Tree) fragmentation

```turtle
<bucketize> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <dataInput>;
    rdfc:metadataInput <metadataInput>;
    rdfc:dataOutput <dataOutput>;
    rdfc:metadataOutput <metadataOutput>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:RTreeFragmentation;
    tree:wktPath <http://www.w3.org/2003/01/geo/wgs84_pos#asWKT>;
    tree:pageSize 100;
    tree:minSize 40;        # Optional, defaults to 40% of pageSize
  ]);
  rdfc:savePath <./buckets_save.json>;
  rdfc:outputStreamId <MyEpicStream>;
  rdfc:prefix "root/".
```

This will create a spatial index using an R-Tree structure.
The `tree:wktPath` specifies the path to the geospatial property, which must be a WKT string in the SDS records.
The `tree:pageSize` specifies the maximum number of entries (members or child nodes) per bucket.
When a bucket exceeds `tree:pageSize`, it is split using the classic [Guttman's Quadratic Split algorithm](https://dl.acm.org/doi/10.1145/971697.602266) to maintain a balanced tree.
The `tree:minSize` specifies the minimum number of entries per bucket to ensure efficient space utilization.
The tree grows dynamically, and buckets are connected using the `tree:GeospatiallyContainsRelation` with their MBR (Minimum Bounding Rectangle) described as a WKT polygon.

Alternatively, you can configure individual latitude and longitude paths:

```turtle
  rdfc:bucketizeStrategy ( [
    a tree:RTreeFragmentation;
    tree:latitudePath <http://example.org/latitude>;
    tree:longitudePath <http://example.org/longitude>;
    tree:pageSize 100;
  ]);
```

In this case, instead of a single spatial relation, the bucketizer generates 4 individual range relations per link to describe the MBR:

- Two `tree:GreaterThanOrEqualToRelation` for the minimum longitude and minimum latitude.
- Two `tree:LessThanOrEqualToRelation` for the maximum longitude and maximum latitude.

### [`rdfc:Ldesify`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/ldesify.ttl#L10)

This processor takes a stream of raw entities (e.g., out from a RML transformation process) and creates versioned entities appending the current timestamp to the entity IRI to make it unique. It is capable of keeping a state so that unmodified entities are filtered.

### [`rdfc:LdesifySDS`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/ldesify.ttl#L82)

Transform SDS-records in SDS-members, creating versioned objects.
The resulting objects are encapsulated in a graph (overriding other graphs).

Specify:

- `rdfc:input` input channel
- `rdfc:output` output channel
- `rdfc:statePath` path for state file
- optional `rdfc:sourceStream`
- `rdfc:targetStream` newly created sds stream id
- optional `rdfc:timestampPath`, defaults to `http://purl.org/dc/terms/modified`
- optional `rdfc:versionOfPath`, defaults to `http://purl.org/dc/terms/isVersionOf`

### [`rdfc:Shapify`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/shapify.ttl#L14)

Execute [Extract CBD Shape algorithm](https://github.com/TREEcg/extract-cbd-shape) on all sds records.
**Note:** this processor does not create a new sds stream.

Specify:

- `rdfc:input` input channel
- `rdfc:output` output channel
- `rdfc:shape` used `sh:NodeShape`

### [`rdfc:MemberAsNamedGraph`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/member_as_graph.ttl#L10)

Transform all sds records payload members into named graph-based members.
**Note:** this processor does not create a new sds stream.

Specify:

- `rdfc:input` input channel
- `rdfc:output` output channel

### [`rdfc:StreamJoin`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/stream_join.ttl#L10)

This processor can be used to join multiple input streams or Reader Channels (`rdfc:input`) and pipe their data flow into a single output stream or Writer Channel (`rdfc:output`). The processor will guarantee that all data elements are delivered downstream and will close the output if all inputs are closed.

### [`rdfc:Generate`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/generator.ttl#L19)

This a simple RDF data generator function used for testing. This processor will periodically generate RDF objects with 3 to 4 predicates.

### [`rdfc:LdesDiskWriter`](https://github.com/rdf-connect/sds-processors-ts/blob/master/configs/ldes_disk_writer.ttl#L8)

This processor can be used to transform an [SDS stream](https://w3id.org/sds/specification) and its correspondent stream of members into a LDES.
It will persist the LDES as a set of files on disk.

Alternative more advanced implementation: [sds-storage-writer-ts](https://github.com/rdf-connect/sds-storage-writer-ts) together with [LDES-Solid-Server](https://github.com/rdf-connect/LDES-Solid-Server).

An example of how to use this processor within a RDF-Connect pipeline definition is shown next:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.

[ ] a rdfc:LdesDiskWriter;
    rdfc:dataInput <data/reader>;
    rdfc:metadataInput <metadata/reader>;
    rdfc:directory </tmp/ldes-disk/>.
```
