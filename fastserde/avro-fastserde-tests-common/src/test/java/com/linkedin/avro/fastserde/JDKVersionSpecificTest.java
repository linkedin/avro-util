package com.linkedin.avro.fastserde;

import org.testng.Assert;
import org.testng.annotations.Test;


public class JDKVersionSpecificTest {
  @Test
  public void jdkVersionTest() {
    try {
      Class<?> cl = Class.forName("com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList");
      if (getVersion() == 11) {
        Assert.assertTrue(cl.getProtectionDomain().getCodeSource().getLocation().toString().contains("fixtures"));
      } else {
        Assert.assertFalse(cl.getProtectionDomain().getCodeSource().getLocation().toString().contains("fixtures"));
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static int getVersion() {
    String version = System.getProperty("java.version");
    if(version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if(dot != -1) { version = version.substring(0, dot); }
    } return Integer.parseInt(version);
  }
}
