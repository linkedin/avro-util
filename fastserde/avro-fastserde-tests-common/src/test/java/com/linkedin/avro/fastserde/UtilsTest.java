package com.linkedin.avro.fastserde;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.generated.avro.FloatWithDefaultNaN;


public class UtilsTest {

  @Test (groups = "deserializationTest")
  public void testGenerateSourcePathFromPackageName() {
    Assert.assertEquals(Utils.generateSourcePathFromPackageName("com.linkedin.avro"), Utils.fixSeparatorsToMatchOS("/com/linkedin/avro/"));
  }

  @Test
  void testFingerprintOfSchemaWithDefaultNaN() {
    // expect no exception is thrown
    Utils.getSchemaFingerprint(FloatWithDefaultNaN.SCHEMA$);
  }
}
