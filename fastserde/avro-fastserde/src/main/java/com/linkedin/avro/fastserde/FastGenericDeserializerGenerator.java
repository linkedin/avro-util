package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


public final class FastGenericDeserializerGenerator<T> extends FastDeserializerGenerator<T, GenericData> {

  public FastGenericDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath, GenericData modelData) {
    super(true, writer, reader, destination, classLoader, compileClassPath, modelData);
  }
}
