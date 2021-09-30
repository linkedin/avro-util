/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.JsonLiterals;
import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests props-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperPropsTest {
  private String unquote(String str) {
    Assert.assertTrue(str.length() >= 2, str);
    Assert.assertTrue(str.startsWith("\""), str);
    Assert.assertTrue(str.endsWith("\""), str);
    return str.substring(1, str.length() - 1);
  }

  private String unescape(String str) {
    return StringEscapeUtils.unescapeJson(str);
  }

  private String toJsonLiteral(String str) {
    return "\"" + StringEscapeUtils.escapeJson(str) + "\"";
  }

  @Test
  public void testRoundTrip() throws Exception {
    // In this test, we explicitly quote and escape all the values (making all of them proper JSON string literals), so
    // it doesn't matter what they were to begin with (valid strings, valid literals of other types, or invalid).
    String[] testCases = new String[]{};
    testCases = ArrayUtils.addAll(testCases, JsonLiterals.STRING_LITERALS);
    testCases = ArrayUtils.addAll(testCases, JsonLiterals.STRING_VALUES);
    testCases = ArrayUtils.addAll(testCases, JsonLiterals.NON_STRING_LITERALS);
    testCases = ArrayUtils.addAll(testCases, JsonLiterals.INVALID_LITERALS);

    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    for (int i = 0; i < testCases.length; ++i) {
      String name = "schema/helper/" + i;
      String value = testCases[i];

      // ------------------------------------
      // Set the property via the helper API.
      AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, name, toJsonLiteral(value), true);

      // Get the property via the helper API, for all combinations of the quoting and escaping options.
      String got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name, false, false);
      got = unescape(got);
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name, false, true);
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name, true, false);
      got = unescape(unquote(got));
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name, true, true);
      got = unquote(got);
      Assert.assertEquals(got, value, name);

      // Get the property via the native API.
      got = schema.getProp(name);
      Assert.assertEquals(got, value, name);

      // ------------------------------------
      // Set the property via the native API.
      name = "schema/native/" + i;
      schema.addProp(name, value);

      // Get the property via the helper API.
      got = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, name);
      got = unescape(unquote(got));
      Assert.assertEquals(got, value, name);
    }

    // ----------------------------------------------------------------------------------------------
    // These tests below are the same as above, except applied to a Schema.Field instead of a Schema.

    Schema.Field field = schema.getField("stringField");
    for (int i = 0; i < testCases.length; ++i) {
      String name = "field/helper/" + i;
      String value = testCases[i];

      // ------------------------------------
      // Set the property via the helper API.
      AvroCompatibilityHelper.setFieldPropFromJsonString(field, name, toJsonLiteral(value), true);

      // Get the property via the helper API, for all combinations of the quoting and escaping options.
      String got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name, false, false);
      got = unescape(got);
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name, false, true);
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name, true, false);
      got = unescape(unquote(got));
      Assert.assertEquals(got, value, name);

      got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name, true, true);
      got = unquote(got);
      Assert.assertEquals(got, value, name);

      // Get the property via the native API.
      got = field.getProp(name);
      Assert.assertEquals(got, value, name);

      // ------------------------------------
      // Set the property via the native API.
      name = "field/native/" + i;
      field.addProp(name, value);

      // Get the property via the helper API.
      got = AvroCompatibilityHelper.getFieldPropAsJsonString(field, name);
      got = unescape(unquote(got));
      Assert.assertEquals(got, value, name);
    }
  }

  @Test
  public void testValidStrings() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    for (int i = 0; i < JsonLiterals.STRING_LITERALS.length; ++i) {
      String name = "schema/" + i;
      String literal = JsonLiterals.STRING_LITERALS[i];
      String value = JsonLiterals.STRING_VALUES[i];

      AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, name, literal, true);
      String got = schema.getProp(name);
      Assert.assertEquals(got, value, name);
    }

    // ----------------------------------------------------------------------------------------------
    // These tests below are the same as above, except applied to a Schema.Field instead of a Schema.

    Schema.Field field = schema.getField("stringField");
    for (int i = 0; i < JsonLiterals.STRING_LITERALS.length; ++i) {
      String name = "field/" + i;
      String literal = JsonLiterals.STRING_LITERALS[i];
      String value = JsonLiterals.STRING_VALUES[i];

      AvroCompatibilityHelper.setFieldPropFromJsonString(field, name, literal, true);
      String got = field.getProp(name);
      Assert.assertEquals(got, value, name);
    }
  }

  @Test
  public void testInvalidJson() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
    for (int i = 0; i < JsonLiterals.INVALID_LITERALS.length; ++i) {
      String name = "schema/" + i;
      String value = JsonLiterals.INVALID_LITERALS[i];

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
    for (int i = 0; i < JsonLiterals.INVALID_LITERALS.length; ++i) {
      String name = "field/" + i;
      String value = JsonLiterals.INVALID_LITERALS[i];

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
}
