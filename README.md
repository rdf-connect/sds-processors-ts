# sds-processors

[![Bun CI](https://github.com/ajuvercr/sds-processors/actions/workflows/build-test.yml/badge.svg)](https://github.com/ajuvercr/sds-processors/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/sds-processors.svg?style=popout)](https://npmjs.com/package/sds-processors)

[Connector Architecture](https://the-connector-architecture.github.io/site/docs/1_Home) Typescript processors for handling operations over [SDS streams](https://treecg.github.io/SmartDataStreams-Spec/). It currently exposes 4 functions:

### [`js:Sdsify`](https://github.com/ajuvercr/sds-processors/blob/master/configs/sdsify.ttl#L10)

This processor takes as input a stream non SDS entities (members) and wrap them inside SDS records. Optionally, a type can be specified to indicate the correct subject.

### [`js:Bucketize`](https://github.com/ajuvercr/sds-processors/blob/master/configs/bucketizer.ttl#L10)

This processor takes as input a stream of SDS records and SDS metadata and proceeds to _bucketize_ them according to a predefined strategy ([see example](https://github.com/ajuvercr/sds-processors/blob/master/bucketizeStrategy.ttl)). The SDS metadata will be also transformed to reflect this transformation. Multiple SDS streams can be present on the incoming data channel.

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


### [`js:Ldesify`](https://github.com/ajuvercr/sds-processors/blob/master/configs/ldesify.ttl#L10)

This processor takes a stream of raw entities (e.g., out from a RML transformation process) and creates versioned entities appending the current timestamp to the entity IRI to make it unique. It is capable of keeping a state so that unmodified entities are filtered.

### [`js:Generate`](https://github.com/ajuvercr/sds-processors/blob/be7134a295eb63e17034b2e3ceea0eaf6ad01770/configs/generator.ttl#L19)

This a simple RDF data generator function used for testing. This processor will periodically generate RDF objects with 3 to 4 predicates.
