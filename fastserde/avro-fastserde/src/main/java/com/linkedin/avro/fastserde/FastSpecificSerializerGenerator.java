package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;


public class FastSpecificSerializerGenerator<T> extends FastSerializerGenerator<T> {

  public FastSpecificSerializerGenerator(Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath) {
    super(false, schema, destination, classLoader, compileClassPath);
  }
}
