/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.testcommon.JsonLiterals;
import com.linkedin.avroutil1.testcommon.TestUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests props-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperProps14Test {

  @Test
  public void testValidNonStrings() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    for (int i = 0; i < JsonLiterals.NON_STRING_LITERALS.length; ++i) {
      String name = "schema/" + i;
      String value = JsonLiterals.NON_STRING_LITERALS[i];

      try {
        AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, name, value, true);
        Assert.fail("expected an error for " + name);
      } catch (IllegalArgumentException | IllegalStateException expected) {
        // expected
      }

      AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, name, value, false);
      String got = schema.getProp(name);
      Assert.assertEquals(got, value, name);
    }

    // ----------------------------------------------------------------------------------------------
    // These tests below are the same as above, except applied to a Schema.Field instead of a Schema.

    Schema.Field field = schema.getField("stringField");
    for (int i = 0; i < JsonLiterals.NON_STRING_LITERALS.length; ++i) {
      String name = "field/" + i;
      String value = JsonLiterals.NON_STRING_LITERALS[i];

      try {
        AvroCompatibilityHelper.setFieldPropFromJsonString(field, name, value, true);
        Assert.fail("expected an error for " + name);
      } catch (IllegalArgumentException | IllegalStateException expected) {
        // expected
      }

      AvroCompatibilityHelper.setFieldPropFromJsonString(field, name, value, false);
      String got = field.getProp(name);
      Assert.assertEquals(got, value, name);
    }
  }

  @Test
  public void testSchemaPropNames() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    List<String> want = Arrays.asList(new String[]{
      "schemaNestedJsonProp",
      "schemaStringProp",
    });
    List<String> got = AvroCompatibilityHelper.getAllPropNames(schema);
    Collections.sort(got);
    Assert.assertEquals(got, want);
  }

  @Test
  public void testFieldPropNames() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    List<String> want = Arrays.asList(new String[]{
      "nestedJsonProp",
      "stringProp",
    });
    List<String> got = AvroCompatibilityHelper.getAllPropNames(schema.getField("stringField"));
    Collections.sort(got);
    Assert.assertEquals(got, want);
  }
}
