/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.fasterxml.jackson.databind.node.NullNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Avro110FieldBuilderTest {

  @Test
  public void testNullDefaultForNullField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("nullWithoutDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultVal());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);

    // Explicit null marker object.
    builder.setDefault(JsonProperties.NULL_VALUE);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);

    // Wrong marker object.
    builder.setDefault(NullNode.getInstance());
    try {
      builder.build();
    } catch (AvroRuntimeException expected) {
      Assert.assertEquals(expected.getMessage(), "Unknown datum class: class com.fasterxml.jackson.databind.node.NullNode");
    }

    // Arbitrary object not valid as the default.
    builder.setDefault("invalid");
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field nullWithoutDefault: \"invalid\" not a \"null\"");
    }

    // Change the schema. The default is not reset, because it's not null.
    Schema.Field anotherField = schema.getField("boolWithoutDefault");
    builder.setSchema(anotherField.schema());
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field nullWithoutDefault: \"invalid\" not a \"boolean\"");
    }

    // Change the schema after setting the default to null.
    builder.setSchema(field.schema());
    builder.setDefault(null);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);
    builder.setSchema(anotherField.schema());
    Assert.assertNull(builder.build().defaultVal());
  }

  @Test
  public void testNullDefaultForUnionWithNullField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("unionWithNullNoDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultVal());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);

    // Explicit null marker object.
    builder.setDefault(JsonProperties.NULL_VALUE);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);

    // Wrong marker object.
    builder.setDefault(NullNode.getInstance());
    try {
      builder.build();
    } catch (AvroRuntimeException expected) {
      Assert.assertEquals(expected.getMessage(), "Unknown datum class: class com.fasterxml.jackson.databind.node.NullNode");
    }

    // Arbitrary object not valid as the default.
    builder.setDefault("invalid");
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field unionWithNullNoDefault: \"invalid\" not a [\"null\",\"string\"]");
    }

    // Change the schema. The default is not reset, because it's not null.
    Schema.Field anotherField = schema.getField("boolWithoutDefault");
    builder.setSchema(anotherField.schema());
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field unionWithNullNoDefault: \"invalid\" not a \"boolean\"");
    }

    // Change the schema after setting the default to null.
    builder.setSchema(field.schema());
    builder.setDefault(null);
    Assert.assertEquals(builder.build().defaultVal(), JsonProperties.NULL_VALUE);
    builder.setSchema(anotherField.schema());
    Assert.assertNull(builder.build().defaultVal());
  }

  @Test
  public void testNullDefaultForBoolField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("boolWithoutDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultVal());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertNull(builder.build().defaultVal());

    // Explicit null marker object.
    builder.setDefault(JsonProperties.NULL_VALUE);
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field boolWithoutDefault: null not a \"boolean\"");
    }

    // Wrong marker object.
    builder.setDefault(NullNode.getInstance());
    try {
      builder.build();
    } catch (AvroRuntimeException expected) {
      Assert.assertEquals(expected.getMessage(), "Unknown datum class: class com.fasterxml.jackson.databind.node.NullNode");
    }

    // Arbitrary object not valid as the default.
    builder.setDefault("invalid");
    try {
      builder.build();
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Invalid default for field boolWithoutDefault: \"invalid\" not a \"boolean\"");
    }
  }

  @Test
  public void testArrayOfEnumDefaultValue() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("FieldWithArrayOfEnumDefaultValue.avsc"));
    Schema.Field field = schema.getField("arrayOfEnum");
    Object defaultValue = AvroCompatibilityHelper.getGenericDefaultValue(field);
    FieldBuilder builder = AvroCompatibilityHelper.newField(field);
    builder.setDefault(defaultValue);

    // Test that .build() should not throw an exception.
    Schema.Field resField = builder.build();
    Assert.assertNotNull(resField.defaultVal());
  }

  @Test
  public void testAddPropsFields() {
    // default (no order specified).
    String propFieldString = "\"IAmAString\"";
    String propFieldObject = "{\"f1\": \"v1\"}";
    Map<String, String> propMap = new HashMap<>();
    propMap.put("string", propFieldString);
    propMap.put("object", propFieldObject);

    Schema.Field field = AvroCompatibilityHelper.newField(null).setName("default").addProps(propMap).build();
    Assert.assertEquals(field.getProp("string"), "IAmAString");
    Assert.assertEquals(field.getObjectProp("object").toString(), "{f1=v1}");
  }
}
