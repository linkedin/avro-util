/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.google.common.base.Throwables;
import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests the default value related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperDefaultsTest {

  @Test
  public void testGetSpecificFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("nullField")));
    Assert.assertTrue((Boolean) AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("boolField")));
    //returns a Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("strField")).toString(), "default");
    Assert.assertNull(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithNullDefault")));
    //Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithStringDefault")).toString(), "def");
  }

  @Test
  public void testGetGenericFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("nullField")));
    Assert.assertTrue((Boolean) AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("boolField")));
    //returns a Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("strField")).toString(), "default");
    Assert.assertNull(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithNullDefault")));
    //Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithStringDefault")).toString(), "def");
  }

  @Test
  public void testGetFieldDefaultsAsJson() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("nullField")), "null");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("boolField")), "true");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("strField")), "\"default\"");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithNullDefault")), "null");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithStringDefault")), "\"def\"");

    schema = by14.HasComplexDefaults.SCHEMA$;
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("fieldWithDefaultRecord")), "{\"intField\":7}");
  }

  @Test
  public void testGetDefaultsForFieldsWithoutDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("nullWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("nullWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("nullWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("nullWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("boolWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("boolWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("boolWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("boolWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }

    try {
      AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }
  }

  @Test
  public void testComplexDefaultValue() throws Exception {
    Schema schema = under14.HasComplexDefaults.SCHEMA$;

    Schema.Field field = schema.getField("fieldWithDefaultEnum");
    Object specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultEnum);
    Object genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.EnumSymbol);

    field = schema.getField("fieldWithDefaultFixed");
    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultFixed);
    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Fixed);

    field = schema.getField("fieldWithDefaultRecord");
    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultRecord);
    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Record);
  }
}
