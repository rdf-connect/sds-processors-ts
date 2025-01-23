# sds-processors

[![Bun CI](https://github.com/rdf-connect/sds-processors/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sds-processors/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sds-processors-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sds-processors-ts)

Collection of [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) Typescript processors for handling [SDS (Smart Data Streams)](https://treecg.github.io/SmartDataStreams-Spec/)-related operations. It currently exposes 5 functions:

### [`js:Sdsify`](https://github.com/rdf-connect/sds-processors/blob/master/configs/sdsify.ttl#L10)

This processor takes as input a stream of (batched) RDF data entities and wraps them as individual SDS records to be further processed downstream. By default, it will extract individual entities by taking every single named node subject and extracting a [Concise Bounded Description](https://www.w3.org/Submission/CBD/) (CBD) of that entity with respect to the input RDF graph.

Alternatively, a set of types may be specified (`js:typeFilter`) to target concrete entities. A SHACL shape can be given to concretely define the bounds target entities and their properties, that want to be extracted and packaged as SDS records. This processor relies on the [member extraction algorithm](https://github.com/TREEcg/extract-cbd-shape) implemented by the [W3C TREE Hypermedia community group](https://www.w3.org/community/treecg/).

If the `js:timestampPath` is specified, the set of SDS records will be streamed out in temporal order to avoid out of order writing issues downstream.

An example of how to use this processor within a RDF-Connect pipeline definition is shown next:

```turtle
@prefix : <https://w3id.org/conn#>.
@prefix js: <https://w3id.org/conn/js#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

[ ] a js:Sdsify;
    js:input <inputChannelReader>;
    js:output <outputChannerWriter>;
    js:stream <http://ex.org/myStream>;
    js:typeFilter ex:SomeClass, ex:SomeOtherClass;
    js:timestampPath <http://ex.org/timestamp>;
    js:shape """
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

### [`js:Bucketize`](https://github.com/rdf-connect/sds-processors/blob/master/configs/bucketizer.ttl#L10)

This processor takes as input a stream of SDS records and SDS metadata and proceeds to _bucketize_ them according to a predefined strategy ([see example](https://github.com/rdf-connect/sds-processors/blob/master/bucketizeStrategy.ttl)). The SDS metadata will be also transformed to reflect this transformation. Multiple SDS streams can be present on the incoming data channel.

You can define bucketizers as follows:

#### Example of a subject and page fragmentation

```turtle
<bucketize> a js:Bucketize;
  js:channels [
    js:dataInput <...data input>;
    js:metadataInput <... metadata input>;
    js:dataOutput <... data output>;
    js:metadataOutput <... metadata output>;
  ];
  js:bucketizeStrategy ( [            # One or more bucketize strategies
    a tree:SubjectFragmentation;      # Create a bucket based on this path
    tree:fragmentationPath ( );
  ] [
    a tree:PageFragmentation;         # Create a new bucket when the previous bucket has 2 members
    tree:pageSize 2;
  ] );
  js:savePath <./buckets_save.json>;
  js:outputStreamId <MyEpicStream>.
```


#### Example of a time-based fragmentation

```turtle
<bucketize> a js:Bucketize;
  js:channels [
    js:dataInput <...data input>;
    js:metadataInput <... metadata input>;
    js:dataOutput <... data output>;
    js:metadataOutput <... metadata output>;
  ];
  js:bucketizeStrategy ( [
    a tree:TimebasedFragmentation;
    tree:timestampPath <https://www.w3.org/ns/activitystreams#published>;
    tree:maxSize 100;
    tree:k 4;
    tree:minBucketSpan 3600;        # In seconds
  ]);
  js:savePath <./buckets_save.json>;
  js:outputStreamId <MyEpicStream>.
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


### [`js:Ldesify`](https://github.com/rdf-connect/sds-processors/blob/master/configs/ldesify.ttl#L10)

This processor takes a stream of raw entities (e.g., out from a RML transformation process) and creates versioned entities appending the current timestamp to the entity IRI to make it unique. It is capable of keeping a state so that unmodified entities are filtered.


### [`js:LdesifySDS`](https://github.com/rdf-connect/sds-processors/blob/master/configs/ldesify.ttl#L82)

Transform SDS-records in SDS-members, creating versioned objects.
The resulting objects are encapsulated in a graph (overriding other graphs).

Specify: 
- `js:input` input channel
- `js:output` output channel
- `js:statePath` path for state file
- optional `js:sourceStream`
- `js:targetStream` newly created sds stream id
- optional `js:timestampPath`, defaults to `http://purl.org/dc/terms/modified`
- optional `js:versionOfPath`, defaults to `http://purl.org/dc/terms/isVersionOf`


### [`js:Shapify`](https://github.com/rdf-connect/sds-processors/blob/master/configs/shapify.ttl#L14)

Execute [Extract CBD Shape algorithm](https://github.com/TREEcg/extract-cbd-shape) on all sds records.
**Note:** this processor does not create a new sds stream.

Specify:
- `js:input` input channel
- `js:output` output channel
- `js:shape` used `sh:NodeShape`

### [`js:MemberAsNamedGraph`](https://github.com/rdf-connect/sds-processors/blob/master/configs/member_as_graph.ttl#L10)

Transform all sds records payload members into named graph-based members.
**Note:** this processor does not create a new sds stream.

Specify:
- `js:input` input channel
- `js:output` output channel


### [`js:StreamJoin`](https://github.com/rdf-connect/sds-processors/blob/master/configs/stream_join.ttl#L10)

This processor can be used to join multiple input streams or Reader Channels (`js:input`) and pipe their data flow into a single output stream or Writer Channel (`js:output`). The processor will guarantee that all data elements are delivered downstream and will close the output if all inputs are closed.

### [`js:Generate`](https://github.com/rdf-connect/sds-processors/blob/master/configs/generator.ttl#L19)

This a simple RDF data generator function used for testing. This processor will periodically generate RDF objects with 3 to 4 predicates.

### [`js:LdesDiskWriter`](https://github.com/rdf-connect/sds-processors/blob/master/configs/ldes_disk_writer.ttl#L8)

This processor can be used to transform an [SDS stream](https://w3id.org/sds/specification) and its correspondent stream of members into a LDES.
It will persist the LDES as a set of files on disk.

Alternative more advanced implementation: [sds-storage-writer-ts](https://github.com/rdf-connect/sds-storage-writer-ts) together with [LDES-Solid-Server](https://github.com/rdf-connect/LDES-Solid-Server).

An example of how to use this processor within a RDF-Connect pipeline definition is shown next:

```turtle
@prefix js: <https://w3id.org/conn/js#>.

[ ] a js:LdesDiskWriter;
    js:dataInput <data/reader>;
    js:metadataInput <metadata/reader>;
    js:directory </tmp/ldes-disk/>.
```
