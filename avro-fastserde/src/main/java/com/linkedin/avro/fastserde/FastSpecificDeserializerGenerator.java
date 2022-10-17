package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class FastSpecificDeserializerGenerator<T> extends FastDeserializerGenerator<T> {

  public FastSpecificDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath) {
    this(writer, reader, destination, classLoader, compileClassPath, Utf8.class);
  }

  public FastSpecificDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath, Class defaultStringClass) {
    super(false, writer, reader, destination, classLoader, compileClassPath, defaultStringClass);
  }
}
