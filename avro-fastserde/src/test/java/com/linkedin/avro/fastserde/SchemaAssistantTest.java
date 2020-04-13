package com.linkedin.avro.fastserde;

import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaAssistantTest {

  @Test
  public void testGetSchemaFullName() {
    String namespace = "com.linkedin.avro.fastserde";
    Assert.assertEquals("STRING", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.STRING)));
    Assert.assertEquals("BYTES", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.BYTES)));
    Assert.assertEquals("INT", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.INT)));
    Assert.assertEquals("LONG", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.LONG)));
    Assert.assertEquals("FLOAT", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.FLOAT)));
    Assert.assertEquals("DOUBLE", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.DOUBLE)));
    Assert.assertEquals("BOOLEAN", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.BOOLEAN)));
    Assert.assertEquals("NULL", SchemaAssistant.getSchemaFullName(Schema.create(Schema.Type.NULL)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestRecord", SchemaAssistant.getSchemaFullName(Schema.createRecord("TestRecord", "", namespace, false)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestFixed", SchemaAssistant.getSchemaFullName(Schema.createFixed("TestFixed", "", namespace, 16)));
    Assert.assertEquals("com.linkedin.avro.fastserde.TestEnum", SchemaAssistant.getSchemaFullName(Schema.createEnum("TestEnum", "", namespace, Collections
        .emptyList())));
  }
}
