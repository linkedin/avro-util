/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;


/**
 * an interface for performing various avro operations that are avro-version-dependant.
 * each implementation is expected to support a given major version of avro (more specifically, the latest
 * minor version of a particular major version - so the adapter for 1.7 actually supports 1.7.7).
 */
public interface AvroAdapter {

  //codecs

  BinaryEncoder newBinaryEncoder(OutputStream out, boolean buffered, BinaryEncoder reuse);

  BinaryEncoder newBinaryEncoder(ObjectOutput out);

  BinaryDecoder newBinaryDecoder(InputStream in, boolean buffered, BinaryDecoder reuse);

  BinaryDecoder newBinaryDecoder(ObjectInput in);

  BinaryDecoder newBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse);

  JsonEncoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty) throws IOException;

  Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat) throws IOException;

  JsonDecoder newJsonDecoder(Schema schema, InputStream in) throws IOException;

  JsonDecoder newJsonDecoder(Schema schema, String in) throws IOException;

  Decoder newCompatibleJsonDecoder(Schema schema, InputStream in) throws IOException;

  Decoder newCompatibleJsonDecoder(Schema schema, String in) throws IOException;

  Decoder newCachedResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException;

  //parsing and Schema-related

  SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known);

  String toParsingForm(Schema s);

  //specific record utils

  Object newInstance(Class<?> clazz, Schema schema);

  Object getSpecificDefaultValue(Schema.Field field);

  //generic record utils

  GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue);

  GenericData.Fixed newFixedField(Schema fixedSchema);

  GenericData.Fixed newFixedField(Schema fixedSchema, byte[] contents);

  Object getGenericDefaultValue(Schema.Field field);

  boolean fieldHasDefault(Schema.Field field);

  //code generation

  Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion
  );
}
