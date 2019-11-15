Getting Started
===============

## Introduction

This lib is a fork of the open-source project: https://github.com/RTBHOUSE/avro-fastserde.
You could get more information regarding the original thinking of this lib by this comprehensive
blog: https://techblog.rtbhouse.com/2017/04/18/fast-avro/.

In the high-level, this is an alternative of the standard Avro serialization/de-serialization
framework, but with better performance by doing runtime code generation.
According to the prod experiment, the de-serlialization time could be reduced by 90% for
some complicated schema, and the serialization time is also improved substantially.

If your application is encountering performance issue with Avro serialization/de-serialization,
you might want to give it a try.

When bringing this lib to LinkedIn, we have done the following improvements so far:
1. Made the lib work with both Avro-1.4 and Avro-1.7;
2. Modified the lib to return "Utf8" instead of "String" to align with
the standard Avro lib;
3. Improved the performance of the original lib by caching the generated
de-serlializer in "FastSpecificDatumReader" and "FastGenericDatumReader";
4. Improved the unit tests to always run the tests against both this fast lib
and the standard Avro lib;
5. Improved the de-serialization generator to support object reuse;
6. Introduced some GC optimization for float vectors;

We will continue to improve this lib in the future since we are seeing this lib could
be widely used.


## Requirements

## Installation/Setting Up

Gradle dependency:
com.linkedin.avroutil:avro-codegen:0.1.1

## Using the Product or Service

Since all the APIs in this lib is compatible with the generic Avro lib, so it is very
straightforward to use them.

For de-serialization, there are two classes:
FastGenericDatumReader
FastSpecificDatumReader

For serialization, there are also two classes:
FastGenericDatumWriter
FastSpecificDatumWriter

Code example:
import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastGenericDatumWriter;
import com.linkedin.avro.fastserde.FastSpecificDatumReader;
import com.linkedin.avro.fastserde.FastSpecificDatumWriter;

...

FastGenericDatumReader<GenericData.Record> fastGenericDatumReader = new FastGenericDatumReader<>(writerSchema, readerSchema);
fastGenericDatumReader.read(reuse, binaryDecoder);

FastGenericDatumWriter<GenericData.Record> fastGenericDatumWriter = new FastGenericDatumWriter<>(schema);
fastGenericDatumWriter.write(data, binaryEncoder);

FastSpecificDatumReader<T> fastSpecificDatumReader = new FastSpecificDatumReader<>(writerSchema, readerSchema);
fastSpecificDatumReader.read(reuse, binaryDecoder);

FastSpecificDatumWriter<T> fastSpecificDatumWriter = new FastSpecificDatumWriter<>(schema);
fastSpecificDatumWriter.write(data, binaryEncoder);



