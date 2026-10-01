package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SchemaAssistantTest {
  @DataProvider
  public Object[][] namedTypes() {
    return new Object[][]{
        {Schema.Type.RECORD, TestRecord.class},
        {Schema.Type.ENUM, TestEnum.class},
        {Schema.Type.FIXED, TestFixed.class}
    };
  }

  @Test(dataProvider = "namedTypes")
  public void testSpecificClassReferencesRejectUnrelatedClasses(Schema.Type type, Class<?> validClass) {
    SchemaAssistant<GenericData> assistant = specificAssistant();
    Assert.assertThrows(SchemaAssistantException.class,
        () -> assistant.classFromSchema(namedSchema(type, String.class), false));
    Assert.assertEquals(assistant.classFromSchema(namedSchema(type, validClass), false).fullName(), validClass.getName());
  }

  @Test
  public void testSpecificValueExpressionsValidateClasses() {
    SchemaAssistant<GenericData> assistant = specificAssistant();
    Schema unrelatedEnum = namedSchema(Schema.Type.ENUM, String.class);
    Schema unrelatedFixed = namedSchema(Schema.Type.FIXED, String.class);
    Assert.assertThrows(SchemaAssistantException.class,
        () -> assistant.getEnumValueByName(unrelatedEnum, JExpr.lit("A"), JExpr._null()));
    Assert.assertThrows(SchemaAssistantException.class,
        () -> assistant.getEnumValueByIndex(unrelatedEnum, JExpr.lit(0), JExpr._null()));
    Assert.assertThrows(SchemaAssistantException.class,
        () -> assistant.getFixedValue(unrelatedFixed, JExpr._null(), JExpr.invoke("getSchema")));

    Schema validEnum = namedSchema(Schema.Type.ENUM, TestEnum.class);
    Assert.assertNotNull(assistant.getEnumValueByName(validEnum, JExpr.lit("A"), JExpr._null()));
    Assert.assertNotNull(assistant.getEnumValueByIndex(validEnum, JExpr.lit(0), JExpr._null()));
    Assert.assertNotNull(assistant.getFixedValue(namedSchema(Schema.Type.FIXED, TestFixed.class),
        JExpr._null(), JExpr.invoke("getSchema")));
  }

  @Test(dataProvider = "namedTypes")
  public void testGenericModeDoesNotRequireSpecificClasses(Schema.Type type, Class<?> unused) {
    SchemaAssistant<GenericData> assistant = new SchemaAssistant<>(
        new JCodeModel(), true, Utf8.class, GenericData.get(), false, getClass().getClassLoader());
    Assert.assertNotNull(assistant.classFromSchema(namedSchema(type, String.class), false));
  }

  private SchemaAssistant<GenericData> specificAssistant() {
    return new SchemaAssistant<>(
        new JCodeModel(), false, Utf8.class, GenericData.get(), false, getClass().getClassLoader());
  }

  private Schema namedSchema(Schema.Type type, Class<?> clazz) {
    String name = clazz.getSimpleName();
    String namespace = clazz.getPackage().getName();
    switch (type) {
      case RECORD:
        Schema record = Schema.createRecord(name, null, namespace, false);
        record.setFields(Collections.emptyList());
        return record;
      case ENUM:
        return Schema.createEnum(name, null, namespace, Collections.singletonList("A"));
      case FIXED:
        return Schema.createFixed(name, null, namespace, 1);
      default:
        throw new IllegalArgumentException("Expected a named schema type: " + type);
    }
  }

  @Test
  public void testGetSchemaFullName() {
    String namespace = "com.linkedin.avro.fastserde";
    Assert.assertEquals("string", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.STRING)));
    Assert.assertEquals("bytes", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.BYTES)));
    Assert.assertEquals("int", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.INT)));
    Assert.assertEquals("long", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.LONG)));
    Assert.assertEquals("float", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.FLOAT)));
    Assert.assertEquals("double", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.DOUBLE)));
    Assert.assertEquals("boolean", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.BOOLEAN)));
    Assert.assertEquals("null", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.NULL)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestRecord", SchemaAssistant.getSchemaFullName(Schema.createRecord("TestRecord", "", namespace, false)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestFixed", SchemaAssistant.getSchemaFullName(Schema.createFixed("TestFixed", "", namespace, 16)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestEnum", SchemaAssistant.getSchemaFullName(Schema.createEnum("TestEnum", "", namespace, Collections
        .emptyList())));
  }
}
