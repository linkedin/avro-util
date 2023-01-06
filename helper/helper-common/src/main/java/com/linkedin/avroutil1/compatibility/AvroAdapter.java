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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
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

  BinaryDecoder newBinaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse);

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

  DatumWriter<?> newSpecificDatumWriter(Schema writer, SpecificData specificData);

  DatumReader<?> newSpecificDatumReader(Schema writer, Schema reader, SpecificData specificData);

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

  boolean defaultValuesEqual(Schema.Field a, Schema.Field b, boolean allowLooseNumerics);

  Set<String> getFieldAliases(Schema.Field field);

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

  /**
   * compare json properties on 2 schema fields for equality.
   * @param a a field
   * @param b another field
   * @param compareStringProps true to compare string properties (otherwise ignored)
   * @param compareNonStringProps true to compare all other properties (otherwise ignored). this exists because avro 1.4
   *                              doesnt handle non-string props at all and we want to be able to match that behaviour under any avro
   * @param jsonPropNamesToIgnore a set of json property names to ignore. if null, no properties will be ignored
   * @return if field properties are equal, under the configuration parameters above
   */
  boolean sameJsonProperties(Schema.Field a, Schema.Field b, boolean compareStringProps, boolean compareNonStringProps,
      Set<String> jsonPropNamesToIgnore);

  String getSchemaPropAsJsonString(Schema schema, String propName);

  void setSchemaPropFromJsonString(Schema field, String propName, String valueAsJsonLiteral, boolean strict);

  /**
   * compare json properties on 2 schemas for equality.
   * @param a a schema
   * @param b another schema
   * @param compareStringProps true to compare string properties (otherwise ignored)
   * @param compareNonStringProps true to compare all other properties (otherwise ignored). this exists because avro 1.4
   *                              doesnt handle non-string props at all and we want to be able to match that behaviour under any avro
   * @param jsonPropNamesToIgnore a set of json property names to ignore. if null, all properties are considered
   * @return if schema properties are equal, under the configuration parameters above
   */
  boolean sameJsonProperties(Schema a, Schema b, boolean compareStringProps, boolean compareNonStringProps,
      Set<String> jsonPropNamesToIgnore);

  List<String> getAllPropNames(Schema schema);

  List<String> getAllPropNames(Schema.Field field);

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

  /**
   * given schemas about to be code-gen'd and a code-gen config, returns a map of
   * alternative AVSC to use by schema full name.
   * @param toCompile schemas about to be "compiled"
   * @param config configuration
   * @return alternative AVSCs, keyed by schema full name
   */
  default Map<String, String> createAlternativeAvscs(Collection<Schema> toCompile, CodeGenerationConfig config) {
    if (!config.isAvro702HandlingEnabled()) {
      return Collections.emptyMap();
    }
    AvscGenerationConfig avscGenConfig = config.getAvro702AvscReplacement();
    Map<String, String> fullNameToAlternativeAvsc = new HashMap<>(1); //expected to be small
    //look for schemas that are susceptible to avro-702, and re-generate their AVSC if required
    for (Schema schema : toCompile) {
      if (!HelperConsts.NAMED_TYPES.contains(schema.getType())) {
        continue; //only named types impacted by avro-702 to begin with
      }
      String fullName = schema.getFullName();
      if (isSusceptibleToAvro702(schema)) {
        String altAvsc = toAvsc(schema, avscGenConfig);
        fullNameToAlternativeAvsc.put(fullName, altAvsc);
      }
    }
    return fullNameToAlternativeAvsc;
  }

  //code generation

  Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion, CodeGenerationConfig config);
}
