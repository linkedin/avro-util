package com.linkedin.avro.fastserde;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public final class FastSpecificDeserializerGenerator<T> extends FastDeserializerGenerator<T, SpecificData> {

  public FastSpecificDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath, SpecificData modelData) {
    super(false, writer, reader, destination, classLoader, compileClassPath, modelData);
  }
}
