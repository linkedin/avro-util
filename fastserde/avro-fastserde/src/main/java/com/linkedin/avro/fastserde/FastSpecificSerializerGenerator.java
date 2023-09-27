package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public class FastSpecificSerializerGenerator<T> extends FastSerializerGenerator<T, SpecificData> {

  public FastSpecificSerializerGenerator(Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath, SpecificData modelData) {
    super(false, schema, destination, classLoader, compileClassPath, modelData);
  }
}
