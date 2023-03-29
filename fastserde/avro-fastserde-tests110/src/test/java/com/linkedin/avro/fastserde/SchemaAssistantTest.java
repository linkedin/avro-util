package com.linkedin.avro.fastserde;

import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaAssistantTest {

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
