package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;


public final class FastGenericDeserializerGenerator<T> extends FastDeserializerGenerator<T> {

  public FastGenericDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath) {
    super(true, writer, reader, destination, classLoader, compileClassPath);
  }
}
