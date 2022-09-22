/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.testcommon.JsonLiterals;
import com.linkedin.avroutil1.testcommon.TestUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests props-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperProps111Test {

  @Test
  public void testValidNonStrings() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    for (int i = 0; i < JsonLiterals.NON_STRING_LITERALS.length; ++i) {
    String name = "schema/" + i;
    String literal = JsonLiterals.NON_STRING_LITERALS[i];
    String value = JsonLiterals.NON_STRING_VALUES[i];

    AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, name, literal, true);
    JsonNode node = JacksonUtils.toJsonNode(schema.getObjectProp(name));
    Assert.assertFalse(node.isTextual(), name);
    Assert.assertEquals(node.toString(), value, name);

    String got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name, false, false);
    Assert.assertEquals(got, value, name);
    }

    // ----------------------------------------------------------------------------------------------
    // These tests below are the same as above, except applied to a Schema.Field instead of a Schema.

    Schema.Field field = schema.getField("stringField");
    for (int i = 0; i < JsonLiterals.NON_STRING_LITERALS.length; ++i) {
    String name = "field/" + i;
    String literal = JsonLiterals.NON_STRING_LITERALS[i];
    String value = JsonLiterals.NON_STRING_VALUES[i];

    AvroCompatibilityHelper.setFieldPropFromJsonString(field, name, literal, true);
    JsonNode node = JacksonUtils.toJsonNode(field.getObjectProp(name));
    Assert.assertFalse(node.isTextual(), name);
    Assert.assertEquals(node.toString(), value, name);

    String got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name, false, false);
    Assert.assertEquals(got, value, name);
    }
  }

  @Test
  public void testSchemaPropNames() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    List<String> want = Arrays.asList(new String[]{
    "schemaBoolProp",
    "schemaFloatProp",
    "schemaIntProp",
    "schemaNestedJsonProp",
    "schemaNullProp",
    "schemaObjectProp",
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
    "boolProp",
    "floatProp",
    "intProp",
    "nestedJsonProp",
    "nullProp",
    "objectProp",
    "stringProp",
    });
    List<String> got = AvroCompatibilityHelper.getAllPropNames(schema.getField("stringField"));
    Collections.sort(got);
    Assert.assertEquals(got, want);
  }
}
