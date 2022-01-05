/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro702CheckerTest {

  @Test
  public void testImpactedSchemas() throws Exception {
    testSchema(TestUtil.load("MonsantoRecord.avsc"), true);
    testSchema(TestUtil.load("avro702/Avro702DemoEnum-good.avsc"), true);
    testSchema(TestUtil.load("avro702/Avro702DemoHorribleEnum-good.avsc"), true);
    testSchema(TestUtil.load("avro702/Avro702DemoFixed-good.avsc"), true);
    testSchema(TestUtil.load("avro702/Avro702DemoRecord-good.avsc"), true);
    testSchema(TestUtil.load("avro702/Avro702DemoUnion-good.avsc"), true);
  }

  @Test
  public void testOtherSchemas() throws Exception {
    testSchema(TestUtil.load("PerfectlyNormalEnum.avsc"), false);
    testSchema(TestUtil.load("PerfectlyNormalFixed.avsc"), false);
    testSchema(TestUtil.load("PerfectlyNormalRecord.avsc"), false);
    testSchema(TestUtil.load("RecordWithDefaults.avsc"), false);
    testSchema(TestUtil.load("RecordWithFieldProps.avsc"), false);
    testSchema(TestUtil.load("RecordWithLogicalTypes.avsc"), false);
  }

  private void testSchema(String avsc, boolean expected) throws Exception {
    Schema schema = Schema.parse(avsc);
    boolean actual = Avro702Checker.isSusceptible(schema);
    Assert.assertEquals(actual, expected);
  }
}
