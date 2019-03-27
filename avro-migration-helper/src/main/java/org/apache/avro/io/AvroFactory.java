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
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonGenerator;


/**
 * a factory interface for performing various avro operations
 * that are avro-version-dependant.
 * this class is in the "org.apache.avro.io" package because some
 * classes used here (like org.apache.avro.ioJsonEncoder) are package-private
 */
public interface AvroFactory {

  BinaryEncoder newBinaryEncoder(OutputStream out);

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

  Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion compatibilityLevel);

  Schema parse(String schemaJson);

  String toParsingForm(Schema s);
}
