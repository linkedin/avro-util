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
  public void testAddNamespaceFillInnerNamespace() throws Exception {
    String nameSpace = "TestDatabase";

    String originalAvsc = TestUtil.load("CCDV.avsc");
    Schema originalSchema = Schema.parse(originalAvsc);

    //original schema does not have namespace
    Assert.assertNull(originalSchema.getNamespace());
    //inner schema does not have namespace
    Assert.assertNull(originalSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace());

    //build new schema and add namespace at top level
    Schema newSchema = AvroCompatibilityHelper.newSchema(originalSchema).setNamespace(nameSpace).build();

    //new schema has namespace
    Assert.assertEquals(newSchema.getNamespace(), nameSpace);
    //inner schema also has same namespace
    Assert.assertEquals(newSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        nameSpace);

    String newAvsc = newSchema.toString(true);
    String expectedAvsc = TestUtil.load("CCDV-namespace.avsc");
    Assert.assertEquals(newAvsc, expectedAvsc);
  }

  @Test
  public void testAddNamespaceNotResetInnerNamespace() throws Exception {
    String nameSpace = "TestDatabase";
    String originalInnerNamespace = "OriginalSpace";

    String originalAvsc = TestUtil.load("CCDV2.avsc");
    Schema originalSchema = Schema.parse(originalAvsc);

    //original schema does not have namespace
    Assert.assertNull(originalSchema.getNamespace());
    //inner schema has namespace
    Assert.assertEquals(originalSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        originalInnerNamespace);

    //build new schema and add namespace at top level
    Schema newSchema = AvroCompatibilityHelper.newSchema(originalSchema).setNamespace(nameSpace).build();

    //new schema has namespace
    Assert.assertEquals(newSchema.getNamespace(), nameSpace);
    //inner schema's namespace does not change
    Assert.assertEquals(newSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        originalInnerNamespace);

    String newAvsc = newSchema.toString(true);
    String expectedAvsc = TestUtil.load("CCDV2-namespace.avsc");
    Assert.assertEquals(newAvsc, expectedAvsc);
  }
}
