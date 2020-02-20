/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */


package com.linkedin.avroutil1.compatibility.avro15;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


public class Avro15SpecificDatumReaderAccessUtil extends SpecificDatumReader<Object> {

  private Avro15SpecificDatumReaderAccessUtil() {
    //this is a util class. dont get any funny ideas
  }

  public static Object newInstancePlease(Class<?> clazz, Schema schema) {
    //see https://stackoverflow.com/questions/24289070/why-we-should-not-use-protected-static-in-java
    return SpecificDatumReader.newInstance(clazz, schema);
  }
}
