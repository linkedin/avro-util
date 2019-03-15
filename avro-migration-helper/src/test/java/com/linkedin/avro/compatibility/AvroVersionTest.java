/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroVersionTest {

  @Test
  public void testLaterThan() throws Exception {
    Assert.assertTrue(AvroVersion.AVRO_1_8.laterThan(AvroVersion.AVRO_1_7));
    Assert.assertFalse(AvroVersion.AVRO_1_7.laterThan(AvroVersion.AVRO_1_8));
  }

  @Test
  public void testParseGoodVersion() throws Exception {
    Assert.assertEquals(AvroVersion.fromSemanticVersion("1.4"), AvroVersion.AVRO_1_4);
    Assert.assertEquals(AvroVersion.fromSemanticVersion("1.8"), AvroVersion.AVRO_1_8);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseBadVersion1() throws Exception {
    AvroVersion.fromSemanticVersion("1");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseBadVersion2() throws Exception {
    AvroVersion.fromSemanticVersion("1.15");
  }
}
