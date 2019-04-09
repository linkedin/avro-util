/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.TestUtil;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class Avro17FactoryTest {
  private final static Pattern BUILDER_METHOD_START = Pattern.compile("newBuilder(\\s*)\\(\\)(\\s*)\\{");
  private final static String BAD_SCHEMA_JSON =
            "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"Bad\",\n"
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
          + "  {\"name\" : \"badInt\", \"type\" : \"int\", \"default\" : \"7\"},\n"
          + "  {\"name\" : \"badBoolean\", \"type\" : \"boolean\", \"default\": \"true\"}\n"
          + "  ]\n"
          + "}";
  private final static String GOOD_SCHEMA_JSON =
            "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"Good\",\n"
          + "  \"namespace\" : \"com.acme\",\n"
          + "  \"fields\" : [\n"
          + "  {\n"
          + "    \"name\" : \"enumField\",\n"
          + "    \"type\" : {\n"
          + "      \"type\" : \"enum\",\n"
          + "      \"name\" : \"GoodEnum\",\n"
          + "      \"symbols\" : [ \"OK\", \"ALSO_OK\" ]\n"
          + "    }\n"
          + "  },\n"
          + "  {\"name\" : \"strField\", \"type\" : \"string\"}\n"
          + "  ]\n"
          + "}";
  private final static String BAD_DEFAULTS_SCHEMA_JSON =
            "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"BadDefaults\",\n"
          + "  \"namespace\" : \"com.acme\",\n"
          + "  \"fields\" : [\n"
          + "  {\"name\" : \"badInt\", \"type\" : \"int\", \"default\" : \"7\"},\n"
          + "  {\"name\" : \"badBoolean\", \"type\" : \"boolean\", \"default\": \"true\"}\n"
          + "  ]\n"
          + "}";

  private Avro17Adapter _factory;


  @BeforeClass
  public void skipForOldAvro() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    List<AvroVersion> supportedVersions = Arrays.asList(AvroVersion.AVRO_1_7, AvroVersion.AVRO_1_8);
    if (!supportedVersions.contains(runtimeVersion)) {
      throw new SkipException("class only supported under " + supportedVersions + ". runtime version detected as " + runtimeVersion);
    }
    _factory = new Avro17Adapter();
  }

  @Test
  public void testStripOutBuilderFrom17Code() throws Exception {
    String generatedCode = TestUtil.load("HasUnions17.java");
    testStripOutBuilder(generatedCode);
  }

  @Test
  public void testStripOutBuilderFrom18Code() throws Exception {
    String generatedCode = TestUtil.load("HasUnions18.java");
    testStripOutBuilder(generatedCode);
  }

  @Test
  public void testParseGoodSchema() throws Exception {
    _factory.parse(GOOD_SCHEMA_JSON, null); //expected to succeed
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testParseBadSchema() throws Exception {
    _factory.parse(BAD_SCHEMA_JSON, null);
  }

  @Test
  public void testParseBadDefaultsSucceedsOn17() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeVersion != AvroVersion.AVRO_1_7) {
      throw new SkipException("only supported under " + AvroVersion.AVRO_1_7 + ". runtime version detected as " + runtimeVersion);
    }
    _factory.parse(BAD_DEFAULTS_SCHEMA_JSON, null); //expected to succeed
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void testParseBadDefaultsFailsOn18() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeVersion != AvroVersion.AVRO_1_8) {
      throw new SkipException("only supported under " + AvroVersion.AVRO_1_8 + ". runtime version detected as " + runtimeVersion);
    }
    _factory.parse(BAD_DEFAULTS_SCHEMA_JSON, null);
  }

  @Test
  public void testSchemaCanonicalization() throws Exception {
    Schema withDocs = Schema.parse(TestUtil.load("HasSymbolDocs.avsc"));
    Schema withoutDocs = Schema.parse(TestUtil.load("HasNoSymbolDocs.avsc"));
    Assert.assertNotEquals(withDocs.toString(true), withoutDocs.toString(true));
    String c1 = _factory.toParsingForm(withDocs);
    String c2 = _factory.toParsingForm(withoutDocs);
    Assert.assertEquals(c1, c2);
  }

  public void testStripOutBuilder(String fromCode) {
    Assert.assertTrue(BUILDER_METHOD_START.matcher(fromCode).find());
    String sansBuilder = Avro17Adapter.removeBuilderSupport(fromCode);
    Assert.assertFalse(BUILDER_METHOD_START.matcher(sansBuilder).find());
  }
}