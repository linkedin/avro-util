/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.testng.Assert;
import org.testng.TestException;
import org.testng.annotations.Test;

public class Avro15FieldBuilderTest {

  @Test
  public void testNullDefaultForNullField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("nullWithoutDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultValue());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertTrue(builder.build().defaultValue().isNull());

    // Explicit null marker object.
    builder.setDefault(NullNode.getInstance());
    Assert.assertTrue(builder.build().defaultValue().isNull());

    // Arbitrary object. Not valid per the schema, but Avro 1.5 doesn't mind.
    builder.setDefault("invalid");
    Assert.assertEquals(builder.build().defaultValue().getTextValue(), "invalid");

    // Change the schema. The default is not reset, because it's not null.
    Schema.Field anotherField = schema.getField("boolWithoutDefault");
    builder.setSchema(anotherField.schema());
    Assert.assertEquals(builder.build().defaultValue().getTextValue(), "invalid");

    // Change the schema after setting the default to null.
    builder.setSchema(field.schema());
    builder.setDefault(null);
    Assert.assertTrue(builder.build().defaultValue().isNull());
    builder.setSchema(anotherField.schema());
    Assert.assertNull(builder.build().defaultValue());
  }

  @Test
  public void testNullDefaultForUnionWithNullField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("unionWithNullNoDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultValue());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertTrue(builder.build().defaultValue().isNull());

    // Explicit null marker object.
    builder.setDefault(NullNode.getInstance());
    Assert.assertTrue(builder.build().defaultValue().isNull());

    // Arbitrary object. Not valid per the schema, but Avro 1.5 doesn't mind.
    builder.setDefault("invalid");
    Assert.assertEquals(builder.build().defaultValue().getTextValue(), "invalid");

    // Change the schema. The default is not reset, because it's not null.
    Schema.Field anotherField = schema.getField("boolWithoutDefault");
    builder.setSchema(anotherField.schema());
    Assert.assertEquals(builder.build().defaultValue().getTextValue(), "invalid");

    // Change the schema after setting the default to null.
    builder.setSchema(field.schema());
    builder.setDefault(null);
    Assert.assertTrue(builder.build().defaultValue().isNull());
    builder.setSchema(anotherField.schema());
    Assert.assertNull(builder.build().defaultValue());
  }

  @Test
  public void testNullDefaultForBoolField() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("RecordWithDefaults.avsc"));
    Schema.Field field = schema.getField("boolWithoutDefault");
    FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(field);

    // No default value specified; cloneSchemaField() doesn't alter that.
    Assert.assertNull(builder.build().defaultValue());

    // Explicitly set null as the default value.
    builder.setDefault(null);
    Assert.assertNull(builder.build().defaultValue());

    // Explicit null marker object. Not valid per the schema, but Avro 1.5
    // doesn't mind.
    builder.setDefault(NullNode.getInstance());
    Assert.assertTrue(builder.build().defaultValue().isNull());

    // Arbitrary object. Not valid per the schema, but Avro 1.5 doesn't mind.
    builder.setDefault("invalid");
    Assert.assertEquals(builder.build().defaultValue().getTextValue(), "invalid");
  }

  @Test
  public void testAddPropsFields() {
    // default (no order specified).
    String propFieldString = "\"IAmAString\"";
    String propFieldObject = "{\"f1\": \"v1\"}";
    Map<String, String> propMap = new HashMap<>();
    propMap.put("string", propFieldString);
    propMap.put("object", propFieldObject);

    FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(null).setName("default");
    fieldBuilder.addProp("string", propFieldString);
    try {
      fieldBuilder.addProp("object", propFieldObject);
      throw new TestException("Test failed. Inserting object should cause exception");
    } catch (IllegalArgumentException expected) {
      //ignore
    }
    Schema.Field field = fieldBuilder.build();

    Assert.assertEquals(field.getProp("string"), "IAmAString");
    Assert.assertNull(field.getProp("object"));
  }
}
