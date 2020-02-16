package com.linkedin.avroutil1.compatibility.avro14;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


public class Avro14SpecificDatumReaderAccessUtil extends SpecificDatumReader<Object> {

  private Avro14SpecificDatumReaderAccessUtil() {
    //this is a util class. dont get any funny ideas
  }

  public static Object newInstancePlease(Class<?> clazz, Schema schema) {
    //see https://stackoverflow.com/questions/24289070/why-we-should-not-use-protected-static-in-java
    return SpecificDatumReader.newInstance(clazz, schema);
  }
}
