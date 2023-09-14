package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


public class FastGenericSerializerGenerator<T> extends FastSerializerGenerator<T, GenericData> {

  public FastGenericSerializerGenerator(Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath, GenericData modelData) {
    super(true, schema, destination, classLoader, compileClassPath, modelData);
  }
}
