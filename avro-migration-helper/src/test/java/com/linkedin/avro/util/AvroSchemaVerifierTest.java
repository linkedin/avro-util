/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.util;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSchemaVerifierTest {

  @Test
  public void testSchemaUnionDefaultValidation() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";
    Schema schema = Schema.parse(schemaStr);

    AvroSchemaVerifier.get().verifyCompatibility(schema, schema);

    schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";
    schema = Schema.parse(schemaStr);

    try {
      AvroSchemaVerifier.get().verifyCompatibility(schema, schema);
      Assert.fail("Default null should fail with int first union field");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().equals("Field KeyRecord:experience has invalid default value. Expecting int, instead got null"));
    }

    schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\", \"default\": \"default_name\"},{\"name\":\"experience\",\"type\":[\"null\", \"int\", \"float\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";
    schema = Schema.parse(schemaStr);
    AvroSchemaVerifier.get().verifyCompatibility(schema, schema);

    schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : [ {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }]\n" +
        "    },\n" +
        "    \"default\" :  [ ] \n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    schema = Schema.parse(schemaStr);
    AvroSchemaVerifier.get().verifyCompatibility(schema, schema);
  }
}
