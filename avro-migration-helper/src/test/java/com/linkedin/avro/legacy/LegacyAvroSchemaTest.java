/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import org.apache.avro.Schema;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class LegacyAvroSchemaTest {
  String badSchemaJson =
            "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"StillRecoverable\",\n"
          + "  \"namespace\" : \"com.acme\",\n"
          + "  \"fields\" : [ {\n"
          + "    \"name\" : \"enumField\",\n"
          + "    \"type\" : {\n"
          + "      \"type\" : \"enum\",\n"
          + "      \"name\" : \"EvilEnum\",\n"
          + "      \"symbols\" : [ \"OK\", \"NOT OK\" ]\n"
          + "    }\n"
          + "  },\n"
          + "  {\"name\" : \"not-ok\", \"type\" : \"string\"},\n"
          + "  {\"name\" : \"@hellNo\", \"type\" : \"string\"},\n"
          + "  {\"name\" : \"twin\", \"type\" : \"string\"},\n"
          + "  {\"name\" : \"twin\", \"type\" : \"string\"},\n"
          + "  {\"name\" : \"badInt1\", \"type\" : \"int\", \"default\" : \"7\"},\n"
          + "  {\"name\" : \"badBoolean\", \"type\" : \"boolean\", \"default\": \"true\"},\n"
          + "  {\"name\" : \"badString1\", \"type\" : \"string\", \"default\": 7},\n"
          + "  {\"name\" : \"badString2\", \"type\" : \"string\", \"default\": true}\n"
          + "  ]\n"
          + "}";

  String horribleSchemaJson =
            "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"BeyondRepair\",\n"
          + "  \"namespace\" : \"com.acme\",\n"
          + "  \"fields\" : [\n"
          + "  {\"name\" : \"badInt1\", \"type\" : \"int\", \"default\" : \"oopsie\"},\n"
          + "  {\"name\" : \"badInt2\", \"type\" : \"int\", \"default\" : false},\n"
          + "  {\"name\" : \"badBoolean1\", \"type\" : \"boolean\", \"default\": \"oopsie\"},\n"
          + "  {\"name\" : \"badBoolean2\", \"type\" : \"boolean\", \"default\": true}\n"
          + "  ]\n"
          + "}";


  @Test
  public void testFixMalformedSchema() throws Exception {
    if (!AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_4)) {
      throw new SkipException("test only valid under modern avro");
    }
    String id = "123";
    LegacyAvroSchema legacyAvroSchema = new LegacyAvroSchema(id, badSchemaJson);
    Assert.assertNotNull(legacyAvroSchema);
    Assert.assertEquals(id, legacyAvroSchema.getOriginalSchemaId());
    JSONAssert.assertEquals("minified json should be equal to input", badSchemaJson, legacyAvroSchema.getOriginalSchemaJson(), true);
  }

  @Test
  public void testMalformedSchemaWorks() throws Exception {
    if (AvroCompatibilityHelper.getRuntimeAvroVersion() != AvroVersion.AVRO_1_4) {
      throw new SkipException("test only valid under avro 1.4");
    }
    String id = "123";
    LegacyAvroSchema legacyAvroSchema = new LegacyAvroSchema(id, badSchemaJson);
    Assert.assertNotNull(legacyAvroSchema);
    Assert.assertEquals(id, legacyAvroSchema.getOriginalSchemaId());
    JSONAssert.assertEquals("minified json should be equal to input", badSchemaJson, legacyAvroSchema.getOriginalSchemaJson(), true);
    Assert.assertEquals(Schema.parse(badSchemaJson), legacyAvroSchema.getFixedSchema());
    Assert.assertNull(legacyAvroSchema.getIssue()); //nothing wrong with it
    Assert.assertTrue(legacyAvroSchema.getTransforms().isEmpty());
  }

  @Test
  public void testFixInvalidDefaultValues() throws Exception {
    if (!AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      throw new SkipException("test only valid under avro 1.8+");
    }
    String id = "123";
    LegacyAvroSchema legacyAvroSchema = new LegacyAvroSchema(id, badSchemaJson);
    Assert.assertNotNull(legacyAvroSchema.getIssue());
    Assert.assertNotNull(legacyAvroSchema.getFixedSchema());
  }

  @Test
  public void testUnfixableInvalidDefaultValues() throws Exception {
    if (!AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      throw new SkipException("test only valid under avro 1.8+");
    }
    String id = "123";
    LegacyAvroSchema legacyAvroSchema = new LegacyAvroSchema(id, horribleSchemaJson);
    Assert.assertNotNull(legacyAvroSchema.getIssue());
    Assert.assertNull(legacyAvroSchema.getFixedSchema());
  }

  @Test
  public void testPartialSchema() throws Exception {
    //no closing paren
    String partialSchema =
              "{"
            + "\"type\":\"record\","
            + "\"name\":\"Record\","
            + "\"namespace\":\"com.acme\","
            + "\"fields\":[{"
            + "   \"name\":\"dummyheader\","
            + "   \"type\":\"string\","
            + "   \"default\":\"\""
            + "},{"
            + "   \"name\":\"leftPred\","
            + "   \"type\":\"string\","
            + "   \"default\":\"\""
            + "},{"
            + "   \"name\":\"rightPred\","
            + "   \"type\":\"string\","
            + "   \"default\":\"\""
            + "}]";
    LegacyAvroSchema schema = new LegacyAvroSchema("123", partialSchema);
    Assert.assertNotNull(schema.getIssue());
    Assert.assertNull(schema.getFixedSchema()); //unfixable
  }

  @Test
  public void testMissingDefinition() throws Exception {
    String badSchema =
          "{"
        + "   \"type\":\"record\","
        + "   \"name\":\"Outer\","
        + "   \"fields\":[{"
        + "      \"name\":\"undefined\","
        + "      \"type\":\"some.undefined.Type\""
        + "   }]"
        + "}";
    LegacyAvroSchema schema = new LegacyAvroSchema("123", badSchema);
    Assert.assertNotNull(schema.getIssue());
    Assert.assertNull(schema.getFixedSchema()); //unfixable
  }

}
