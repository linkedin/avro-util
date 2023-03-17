package com.linkedin.avro.fastserde;

import org.testng.annotations.Test;


public class JDKVersionTest {
  @Test
  public void jdkVersionTest() {
    try {
      Class<?> cl = Class.forName("com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList");
      System.out.println(cl.getProtectionDomain());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
