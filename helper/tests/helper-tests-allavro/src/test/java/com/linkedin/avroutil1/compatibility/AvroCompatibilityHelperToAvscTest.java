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


/**
 * Tests avsc generation on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperToAvscTest {

  @Test
  public void testMonsantoSchema() throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    testSchema(avsc);

    Schema schema = Schema.parse(avsc);
    String badAvsc = AvroCompatibilityHelper.toBadAvsc(schema, true);
    Schema evilClone = Schema.parse(badAvsc);

    //show avro-702
    Schema correctInnerRecordSchema = schema.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(correctInnerRecordSchema.getNamespace(), "com.acme.outer");
    Schema evilInnerRecordSchema = evilClone.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(evilInnerRecordSchema.getNamespace(), "com.acme.middle");
  }

  @Test
  public void testOtherSchemas() throws Exception {
    testSchema(TestUtil.load("PerfectlyNormalEnum.avsc"));
    testSchema(TestUtil.load("PerfectlyNormalFixed.avsc"));
    testSchema(TestUtil.load("PerfectlyNormalRecord.avsc"));
    testSchema(TestUtil.load("RecordWithDefaults.avsc"));
    testSchema(TestUtil.load("RecordWithFieldProps.avsc"));
    testSchema(TestUtil.load("RecordWithLogicalTypes.avsc"));
  }

  private void testSchema(String avsc) throws Exception {
    Schema schema = Schema.parse(avsc);
    String oneLine = AvroCompatibilityHelper.toAvsc(schema, false);
    String pretty = AvroCompatibilityHelper.toAvsc(schema, true);

    Schema copy = Schema.parse(oneLine);
    Assert.assertEquals(copy, schema);

    copy = Schema.parse(pretty);
    Assert.assertEquals(copy, schema);
  }
}
