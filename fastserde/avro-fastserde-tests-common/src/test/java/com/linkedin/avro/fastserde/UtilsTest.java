package com.linkedin.avro.fastserde;

import org.testng.Assert;
import org.testng.annotations.Test;


public class UtilsTest {

  @Test (groups = "deserializationTest")
  public void testGenerateSourcePathFromPackageName() {
    Assert.assertEquals(Utils.generateSourcePathFromPackageName("com.linkedin.avro"), "/com/linkedin/avro/");
  }
}
