# sds-processors

[![Bun CI](https://github.com/rdf-connect/sds-processors/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sds-processors/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sds-processors.svg?style=popout)](https://npmjs.com/package/@rdfc/sds-processors)

Collection of [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) Typescript processors for handling [SDS (Smart Data Streams)](https://treecg.github.io/SmartDataStreams-Spec/)-related operations. It currently exposes 5 functions:

### [`js:Sdsify`](https://github.com/rdf-connect/sds-processors/blob/master/configs/sdsify.ttl#L10)

This processor takes as input a stream of (batched) RDF data entities and wraps them as individual SDS records to be further processed downstream. By default, it will extract individual entities by taking every single named node subject and extracting a [Concise Bounded Description](https://www.w3.org/Submission/CBD/) (CBD) of that entity with respect to the input RDF graph.

Alternatively, a set of SHACL shapes can be given to concretely define and filter the type of entities and their properties, that want to be extracted and packaged as SDS records. This processor relies on the [member extraction algorithm](https://github.com/TREEcg/extract-cbd-shape) implemented by the [W3C TREE Hypermedia community group](https://www.w3.org/community/treecg/).

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
    js:timestampPath <http://ex.org/timestamp>;
    js:shapeFilter """
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeClass;
            sh:property [ sh:path ex:someProperty ].
    """,
    """
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex: <http://ex.org/>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:SomeOtherClass;
            sh:property [ sh:path ex:someOtherProperty ].
    """.
```

### [`js:Bucketize`](https://github.com/rdf-connect/sds-processors/blob/master/configs/bucketizer.ttl#L10)

This processor takes as input a stream of SDS records and SDS metadata and proceeds to _bucketize_ them according to a predefined strategy ([see example](https://github.com/rdf-connect/sds-processors/blob/master/bucketizeStrategy.ttl)). The SDS metadata will be also transformed to reflect this transformation. Multiple SDS streams can be present on the incoming data channel.

You can define bucketizers as follows:

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


### [`js:Ldesify`](https://github.com/rdf-connect/sds-processors/blob/master/configs/ldesify.ttl#L10)

This processor takes a stream of raw entities (e.g., out from a RML transformation process) and creates versioned entities appending the current timestamp to the entity IRI to make it unique. It is capable of keeping a state so that unmodified entities are filtered.

### [`js:StreamJoin`](https://github.com/rdf-connect/sds-processors/blob/master/configs/stream_join.ttl#L10)

This processor can be used to join multiple input streams or Reader Channels (`js:input`) and pipe their data flow into a single output stream or Writer Channel (`js:output`). The processor will guarantee that all data elements are delivered downstream and will close the output if all inputs are closed.

### [`js:Generate`](https://github.com/rdf-connect/sds-processors/blob/master/configs/generator.ttl#L19)

This a simple RDF data generator function used for testing. This processor will periodically generate RDF objects with 3 to 4 predicates.
