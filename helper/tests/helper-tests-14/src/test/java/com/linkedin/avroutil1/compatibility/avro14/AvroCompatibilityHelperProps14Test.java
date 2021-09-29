/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * tests props-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperProps14Test {

  @Test
  public void testPropSetting() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    Schema.Field stringField = schema.getField("stringField");
    stringField.addProp("propA", "valueA");
    String valueA = AvroCompatibilityHelper.getFieldPropAsJsonString(stringField, "propA", true, false);
    Assert.assertEquals(valueA, "\"valueA\"");
    valueA = AvroCompatibilityHelper.getFieldPropAsJsonString(stringField, "propA", false, false);
    Assert.assertEquals(valueA, "valueA");

    AvroCompatibilityHelper.setFieldPropFromJsonString(stringField, "propB", "valueB", false);
    String valueB = stringField.getProp("propB");
    Assert.assertEquals(valueB, "valueB");
    AvroCompatibilityHelper.setFieldPropFromJsonString(stringField, "propC", "\"valueC\"", true);
    String valueC = stringField.getProp("propC");
    Assert.assertEquals(valueC, "valueC");

    try {
      AvroCompatibilityHelper.setFieldPropFromJsonString(stringField, "propD", "valueD", true);
      Assert.fail("should have thrown");
    } catch (IllegalArgumentException expected) {
      //expected
    }

    try {
      //avro 1.4 doesnt support anything except string props
      AvroCompatibilityHelper.setFieldPropFromJsonString(stringField, "intProp", "7", true);
      Assert.fail("should have thrown");
    } catch (IllegalArgumentException expected) {
      //expected
    }
  }
}
