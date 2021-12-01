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
  public void testMonsantoSchema() throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    testSchema(avsc, true);
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
