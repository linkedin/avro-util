/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro18Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_8;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_8;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro18SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro18SchemaConstructable);
    Avro18SchemaConstructable constructable = (Avro18SchemaConstructable)instance;
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
    ObjectMapper mapper = new ObjectMapper();
    Schema schema = Schema.parse(TestUtil.load("RecordWithRecursiveTypesAndDefaults.avsc"));
    // Test null default value
    Schema.Field field = schema.getField("unionWithNullDefault");
    Assert.assertTrue(AvroCompatibilityHelper.createSchemaField("unionWithNullDefault", field.schema(), "", null).defaultValue().isNull());
    // Test primitive default value
    field = schema.getField("doubleFieldWithDefault");
    Assert.assertEquals(AvroCompatibilityHelper.createSchemaField("doubleFieldWithDefault", field.schema(), "", 1.0).defaultValue().getDoubleValue(), 1.0);
    // Test map default value
    field = schema.getField("mapOfArrayWithDefault");
    Map<String, List<String>> defaultMapValue = Collections.singletonMap("dummyKey", Collections.singletonList("dummyValue"));
    JsonNode actualJsonNode = AvroCompatibilityHelper.createSchemaField("mapOfArrayWithDefault", field.schema(), "", defaultMapValue).defaultValue();
    Map<String, List<String>> actualMapValue = mapper.convertValue(actualJsonNode, new TypeReference<Map<String, List<String>>>(){});
    Assert.assertEquals(actualMapValue.get("dummyKey").get(0), "dummyValue");
    // Test array default value
    field = schema.getField("arrayOfArrayWithDefault");
    List<List<String>> defaultListValue = Collections.singletonList(Collections.singletonList("dummyElement"));
    actualJsonNode = AvroCompatibilityHelper.createSchemaField("arrayOfArrayWithDefault", field.schema(), "", defaultListValue).defaultValue();
    List<List<String>> actualListValue = mapper.convertValue(actualJsonNode, new TypeReference<List<List<String>>>(){});
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
