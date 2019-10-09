/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.Collection;

import com.linkedin.avro.compatibility.SchemaParseResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonGenerator;


/**
 * an interface for performing various avro operations that are avro-version-dependant.
 * this class is in the "org.apache.avro.io" package because some
 * classes used here (like {@link org.apache.avro.io.JsonEncoder}) are package-private
 * under some versions of avro
 */
public interface AvroAdapter {

  BinaryEncoder newBinaryEncoder(OutputStream out);

  BinaryDecoder newBinaryDecoder(InputStream in);

  default JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
    return new JsonEncoder(schema, out); //was made package-private in 1.7
  }

  default JsonEncoder newJsonEncoder(Schema schema, JsonGenerator jsonGenerator) throws IOException {
    return new JsonEncoder(schema, jsonGenerator); //was made package-private in 1.7
  }

  default JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
    return new JsonDecoder(schema, input); //was made package-private in 1.7
  }

  default JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
    return new JsonDecoder(schema, input); //was made package-private in 1.7
  }

  GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue);

  default GenericData.Fixed newFixedField(Schema ofType) {
    byte[] emptyDataArray = new byte[ofType.getFixedSize()]; //null is probably unsafe
    return newFixedField(ofType, emptyDataArray);
  }

  GenericData.Fixed newFixedField(Schema ofType, byte[] contents);

  Object newInstance(Class clazz, Schema schema);

  Schema getSchema(Type type);

  Object getDefaultValue(Schema.Field field);

  /**
   * invokes the avro {@link org.apache.avro.specific.SpecificCompiler} to generate java code
   * and then (optionally) post-processes that code to make it compatible with a wider range
   * of avro versions at runtime.
   * @param toCompile schemas to generate java code out of
   * @param minSupportedRuntimeVersion minimum avro version the resulting code should work under.
   *                                   null means no post-processing will be done at all
   *                                   (resulting in "raw" avro output)
   * @return generated java code
   * @throws UnsupportedOperationException if the {@link org.apache.avro.specific.SpecificCompiler} class
   *                                       is not found on the classpath (which is possible with avro 1.5+)
   */
  Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedRuntimeVersion);

  SchemaParseResult parse(String schemaJson, Collection<Schema> known);

  String toParsingForm(Schema s);
}
