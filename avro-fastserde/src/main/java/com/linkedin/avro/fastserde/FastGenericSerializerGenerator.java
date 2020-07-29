package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;


public class FastGenericSerializerGenerator<T> extends FastSerializerGenerator<T> {

  public FastGenericSerializerGenerator(Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath) {
    super(true, schema, destination, classLoader, compileClassPath);
  }

  public FastGenericSerializerGenerator(Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath, int loadClassLimit) {
    super(true, schema, destination, classLoader, compileClassPath, loadClassLimit);
  }
}
