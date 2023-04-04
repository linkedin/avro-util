package com.linkedin.avro.fastserde;

import java.lang.reflect.Field;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JDKVersionSpecificTest {
  @Test
  /**
   * This test verifies that code is using proper JDK specific version of BufferBackedPrimitiveFloatList class
   * If version is incorrect this means that multi-release jar logic is not working
   */
  public void jdkVersionTest() {
    try {
      Class<?> cl = Class.forName("com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList");
      System.out.println(cl.getProtectionDomain());
      Field buffer = cl.getDeclaredField("byteBuffer");
      buffer.setAccessible(true);
      if (getVersion() >= 11) {
        Assert.assertTrue(buffer.getType().isAssignableFrom(byte[].class));
      } else {
        Assert.assertTrue(buffer.getType().isAssignableFrom(CompositeByteBuffer.class));
      }
    } catch (ClassNotFoundException | NoSuchFieldException e) {
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
