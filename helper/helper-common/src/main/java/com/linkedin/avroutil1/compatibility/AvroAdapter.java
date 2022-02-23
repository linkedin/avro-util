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
import java.util.List;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * an interface for performing various avro operations that are avro-version-dependant.
 * each implementation is expected to support a given major version of avro (more specifically, the latest
 * minor version of a particular major version - so the adapter for 1.7 actually supports 1.7.7).
 */
public interface AvroAdapter {

  //metadata

  AvroVersion supportedMajorVersion();

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

  SkipDecoder newCachedResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException;

  Decoder newBoundedMemoryDecoder(InputStream in) throws IOException;

  Decoder newBoundedMemoryDecoder(byte[] data) throws IOException;

  <T> SpecificDatumReader<T> newAliasAwareSpecificDatumReader(Schema writer, Class<T> readerClass);

  //parsing and Schema-related

  SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known);

  String toParsingForm(Schema s);

  String getDefaultValueAsJsonString(Schema.Field field);

  //specific record utils

  Object newInstance(Class<?> clazz, Schema schema);

  Object getSpecificDefaultValue(Schema.Field field);

  //generic record utils

  GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue);

  GenericData.Fixed newFixedField(Schema fixedSchema);

  GenericData.Fixed newFixedField(Schema fixedSchema, byte[] contents);

  Object getGenericDefaultValue(Schema.Field field);

  //schema query and manipulation utils

  boolean fieldHasDefault(Schema.Field field);

  @Deprecated
  default FieldBuilder cloneSchemaField(Schema.Field field) {
    return newFieldBuilder(field);
  }

  /**
   * constructs a new {@link FieldBuilder}, optionally using a given {@link org.apache.avro.Schema.Field}
   * for initial values
   * @param other a starting point. or null.
   * @return a new builder
   */
  FieldBuilder newFieldBuilder(Schema.Field other);

  @Deprecated
  default FieldBuilder newFieldBuilder(String name) {
    return newFieldBuilder((Schema.Field) null).setName(name);
  }

  /**
   * constructs a new {@link SchemaBuilder}, optionally using a given {@link Schema} for initial values
   * @param other a starting point. or null.
   * @return a new builder
   */
  SchemaBuilder newSchemaBuilder(Schema other);

  String getFieldPropAsJsonString(Schema.Field field, String propName);

  void setFieldPropFromJsonString(Schema.Field field, String propName, String valueAsJsonLiteral, boolean strict);

  String getSchemaPropAsJsonString(Schema schema, String propName);

  void setSchemaPropFromJsonString(Schema field, String propName, String valueAsJsonLiteral, boolean strict);

  String getEnumDefault(Schema s);

  default Schema newEnumSchema(String name, String doc, String namespace, List<String> values, String enumDefault) {
    if (enumDefault != null) {
      //TODO - implement via properties
      throw new AvroTypeException("enum default is not supported in " + supportedMajorVersion().toString());
    }
    return Schema.createEnum(name, doc, namespace, values);
  }

  /**
   * "serialize" a {@link Schema} to avsc format
   * @param schema a schema to print out as (exploded/fully-defined) avsc
   * @return the given schema as avsc
   */
  String toAvsc(Schema schema, AvscGenerationConfig config);

  default boolean isSusceptibleToAvro702(Schema schema) {
    //this implementation is slow, as it completely serializes the schema and parses it back again
    //and potentially catches an exception. its possible to write a stripped down version of the
    //AvscWriter recursion that will simply return a boolean if pre- and post-702 states ever "diverge"
    String naiveAvsc = toAvsc(schema, AvscGenerationConfig.LEGACY_ONELINE);
    boolean parseFailed = false;
    Schema evilTwin = null;
    try {
      evilTwin = Schema.parse(naiveAvsc); //avro-702 can result in "exploded" schemas that dont parse
    } catch (Exception ignored) {
      parseFailed = true;
    }
    return parseFailed || !evilTwin.equals(schema);
  }

  //code generation

  Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion,
      CodeGenerationConfig config
  );
}
