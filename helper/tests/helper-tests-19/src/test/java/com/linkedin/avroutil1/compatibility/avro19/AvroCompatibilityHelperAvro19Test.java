/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro19Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_9;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_9;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro19SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro19SchemaConstructable);
    Avro19SchemaConstructable constructable = (Avro19SchemaConstructable)instance;
    Assert.assertEquals(constructable.getSchema(), schema);
  }

  @Test
  public void testNonSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Pojo.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Pojo);
  }

  @Test
  public void testCreateSchemaFieldWithProvidedDefaultValue() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("RecordWithRecursiveTypesAndDefaults.avsc"));
    // Test null default value
    Schema.Field field = schema.getField("unionWithNullDefault");
    Assert.assertEquals(AvroCompatibilityHelper.createSchemaField("unionWithNullDefault", field.schema(), "", null).defaultVal(), JsonProperties.NULL_VALUE);
    // Test primitive default value
    field = schema.getField("doubleFieldWithDefault");
    Assert.assertEquals(AvroCompatibilityHelper.createSchemaField("doubleFieldWithDefault", field.schema(), "", field.defaultVal()).defaultVal(), 1.0);
    // Test map default value
    field = schema.getField("mapOfArrayWithDefault");
    Map<String, List<String>> actualMapValue =
        (Map<String, List<String>>) AvroCompatibilityHelper.createSchemaField("mapOfArrayWithDefault", field.schema(), "", field.defaultVal()).defaultVal();
    Assert.assertEquals(actualMapValue.get("dummyKey").get(0), "dummyValue");
    // Test array default value
    field = schema.getField("arrayOfArrayWithDefault");
    List<List<String>> actualListValue =
        (List<List<String>>) AvroCompatibilityHelper.createSchemaField("arrayOfArrayWithDefault", field.schema(), "", field.defaultVal()).defaultVal();
    Assert.assertEquals(actualListValue.get(0).get(0), "dummyElement");
  }

  @Test
  public void testCanonicalizeInitialParsing() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroCompatibilityHelper.canonicalize(schema, Arrays.asList("very_important", "record_level_important_json"));
    Schema canonicalizedSchema = Schema.parse(str);

    // Non null
    Assert.assertNotNull(canonicalizedSchema);

    // No docs
    Assert.assertNull(canonicalizedSchema.getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(0).schema().getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(1).schema().getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(2).schema().getDoc());

    //Copies default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(0).hasDefaultValue());
    Assert.assertEquals(canonicalizedSchema.getFields().get(1).defaultVal(), "A");
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).defaultVal().toString(), "{innerLongField=420}");

    // Copies top level record alias
    Assert.assertTrue(canonicalizedSchema.getAliases().contains("com.acme.record_alias"));

    // Copies field schema aliases
    Assert.assertTrue(canonicalizedSchema.getFields().get(1).schema().getAliases().contains("com.acme.field_type_alias"));

    // Copies field level aliases
    Assert.assertTrue(canonicalizedSchema.getFields().get(2).aliases().contains("field_alias"));

    // Copies specified junk json/extra json props from Field
    Assert.assertNotNull(schema.getFields().get(2).schema().getFields().get(0).getObjectProp("very_important"));
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).schema().getFields().get(0).getObjectProp("very_important"),
        schema.getFields().get(2).schema().getFields().get(0).getObjectProp("very_important"));

    // Ignores other junk json/extra json props from field level
    Assert.assertNotNull(schema.getFields().get(0).getProp("not_important_stuff"));
    Assert.assertNull(canonicalizedSchema.getFields().get(0).getProp("not_important_stuff"));

    // Ignores other junk json/extra json props from Top level record
    Assert.assertNotNull(schema.getProp("record_level_junk_json"));
    Assert.assertNull(canonicalizedSchema.getProp("record_level_junk_json"));

    // Copies specified junk json/extra json props from Top level record
    Assert.assertNotNull(schema.getObjectProp("record_level_important_json"));
    Assert.assertEquals(schema.getObjectProp("record_level_important_json"),
        canonicalizedSchema.getObjectProp("record_level_important_json"));
  }
}
