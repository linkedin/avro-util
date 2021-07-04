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
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithNoDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithNoDefault"));
    }
  }

  @Test
  public void testComplexDefaultValue() throws Exception {
    Schema schema = by14.HasComplexDefaults.SCHEMA$;

    Schema.Field field = schema.getField("fieldWithDefaultEnum");
    Object specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof by14.DefaultEnum);
    Object genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.EnumSymbol);

    //avro-1.4 fixed classes cant even be instantiated under avro 1.5+
    //TODO - update this test to use compat generated code
//    field = schema.getField("fieldWithDefaultFixed");
//    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
//    Assert.assertNotNull(specificDefault);
//    Assert.assertTrue(specificDefault instanceof by14.DefaultFixed);
//    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
//    Assert.assertNotNull(genericDefault);
//    Assert.assertTrue(genericDefault instanceof GenericData.Fixed);

    field = schema.getField("fieldWithDefaultRecord");
    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof by14.DefaultRecord);
    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Record);
  }
}
