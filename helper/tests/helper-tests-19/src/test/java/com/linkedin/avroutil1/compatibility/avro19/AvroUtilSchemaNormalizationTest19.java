/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.fasterxml.jackson.core.JsonGenerator;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.ConfigurableSchemaComparator;
import com.linkedin.avroutil1.compatibility.Jackson2JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.normalization.AvroUtilSchemaNormalization;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilSchemaNormalizationTest19 {

  @Test
  public void testCanonicalizeBareBones() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
    Schema canonicalizedSchema = Schema.parse(str);
    Schema schemaFromAvroNormalizer = Schema.parse(org.apache.avro.SchemaNormalization.toParsingForm(schema));

    Schema schemaWithoutSpecialStrProp = Schema.parse(TestUtil.load("RecordWithoutSpecialStrProp.avsc"));
    String strWithoutSpecialStrProp = AvroUtilSchemaNormalization.getCanonicalForm(schemaWithoutSpecialStrProp, AvscGenerationConfig.CANONICAL_ONELINE, null);
    Schema canonicalizedSchemaWithoutSpecialStrProp = Schema.parse(strWithoutSpecialStrProp);
    Schema schemaFromAvroNormalizerWithoutSpecialStrProp = Schema.parse(org.apache.avro.SchemaNormalization.toParsingForm(schemaWithoutSpecialStrProp));

    //  Equal
    Assert.assertTrue(ConfigurableSchemaComparator.equals(canonicalizedSchemaWithoutSpecialStrProp, schemaFromAvroNormalizerWithoutSpecialStrProp, SchemaComparisonConfiguration.STRICT));
    // Not equal due to special str prop
    Assert.assertFalse(ConfigurableSchemaComparator.equals(canonicalizedSchema, schemaFromAvroNormalizer, SchemaComparisonConfiguration.STRICT));

    // Non null
    Assert.assertNotNull(canonicalizedSchema);

    // Docs existed
    Assert.assertNotNull(schema.getDoc());
    Assert.assertNotNull(schema.getFields().get(1).doc());
    Assert.assertNotNull(schema.getFields().get(2).schema().getDoc());
    // No docs
    Assert.assertNull(canonicalizedSchema.getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(1).doc());
    Assert.assertNull(canonicalizedSchema.getFields().get(2).schema().getDoc());

    // Default value existed
    Assert.assertTrue(schema.getFields().get(2).hasDefaultValue());
    Assert.assertTrue(schema.getFields().get(3).hasDefaultValue());
    // No default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(2).hasDefaultValue());
    Assert.assertFalse(canonicalizedSchema.getFields().get(3).hasDefaultValue());

    // Doesn't copy top level record alias
    Assert.assertTrue(schema.getAliases().contains("com.acme.record_alias"));
    Assert.assertFalse(canonicalizedSchema.getAliases().contains("com.acme.record_alias"));

    // Doesn't copy field schema aliases
    Assert.assertTrue(schema.getFields().get(2).schema().getAliases().contains("com.acme.field_type_alias"));
    Assert.assertFalse(canonicalizedSchema.getFields().get(2).schema().getAliases().contains("com.acme.field_type_alias"));

    // Doesn't copy field level aliases
    Assert.assertTrue(schema.getFields().get(3).aliases().contains("field_alias"));
    Assert.assertFalse(canonicalizedSchema.getFields().get(3).aliases().contains("field_alias"));

    // Doesn't copy specified junk json/extra json props from Field
    Assert.assertNotNull(schema.getFields().get(3).schema().getFields().get(0).getObjectProp("very_important"));
    Assert.assertNull(canonicalizedSchema.getFields().get(3).schema().getFields().get(0).getObjectProp("very_important"));

    // Ignores other junk json/extra json props from field level
    Assert.assertNotNull(schema.getFields().get(1).getProp("not_important_stuff"));
    Assert.assertNull(canonicalizedSchema.getFields().get(1).getProp("not_important_stuff"));

    // Ignores other junk json/extra json props from Top level record
    Assert.assertNotNull(schema.getProp("record_level_junk_json"));
    Assert.assertNull(canonicalizedSchema.getProp("record_level_junk_json"));

    // Doesn't copy specified junk json/extra json props from Top level record
    Assert.assertNotNull(schema.getObjectProp("record_level_important_json"));
    Assert.assertNull(canonicalizedSchema.getObjectProp("record_level_important_json"));
  }

  @Test
  public void testCanonicalBroadNoPlugin() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, null);
    Schema canonicalizedSchema = Schema.parse(str);

    //Copies default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(0).hasDefaultValue());
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).defaultVal(), "A");
    Assert.assertEquals(canonicalizedSchema.getFields().get(3).defaultVal().toString(), "{innerLongField=420}");

    // Copies top level record alias, fully qualified and sorted
    Assert.assertTrue(canonicalizedSchema.getAliases().toString().equals("[com.acme.a_record_alias, com.acme.record_alias]"));

    // Copies field schema aliases, and is now sorted and fully qualified
    Assert.assertTrue(canonicalizedSchema.getFields().get(2).schema().getAliases().toString().equals("[com.acme.Afield_type_alias, com.acme.field_type_alias]"));

    // Copies field level aliases, and is now sorted
    Assert.assertTrue(canonicalizedSchema.getFields().get(3).aliases().toString().equals("[afield_alias, field_alias]"));
  }

  @Test
  public void testCanonicalStrictWithNonSpecificJsonIncluded() throws IOException {
    AvscGenerationConfig config = new AvscGenerationConfig(
        false, false, false, Optional.of(Boolean.TRUE), false, false,
        false, false, true, false, true, false);

    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, config, null);
    Schema canonicalizedSchema = Schema.parse(str);

    // copied all props, sorted by key
    Assert.assertEquals(new TreeMap(schema.getObjectProps()), canonicalizedSchema.getObjectProps());

    // for fields
    for(int fieldPos = 0; fieldPos < schema.getFields().size(); fieldPos ++) {
      Schema.Field field = schema.getFields().get(fieldPos);
      Schema.Field canonicalizedField = canonicalizedSchema.getFields().get(fieldPos);

      if(field.hasProps()) {
        Assert.assertEquals(new TreeMap<>(field.getObjectProps()), canonicalizedField.getObjectProps());
      }

      // and for field schemas
      if(field.schema().hasProps()) {
        Assert.assertEquals(new TreeMap<>(field.schema().getObjectProps()), canonicalizedField.schema().getObjectProps());
      }
    }

  }

  /***
   * Test that written avsc follows the following order:
   * "name"
   * "namespace"
   * "type"
   * "fields"
   * "symbols"
   * "items"
   * "values"
   * "size"
   * "default"
   * "order"
   * "aliases"
   * "doc"
   * "included json - via plugins in order of plugins"
   * "extra json - alphabetically sorted by key"
   * @throws IOException
   */
  @Test
  public void testOrder() throws IOException {

    AvscGenerationConfig config = new AvscGenerationConfig(
        false, false, false, Optional.of(Boolean.TRUE), false, true,
        true, true, true, true, true, false);

    String expectedForm =
        "{"
            + "\"name\":\"OrderTester\","
            + "\"namespace\":\"vs14\","
            + "\"type\":\"record\","
            + "\"fields\":["
              + "{"
                + "\"name\":\"stringField\","
                + "\"type\":\"string\","
                + "\"doc\":\"Doc\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"exception\","
                + "\"type\":\"float\","
                + "\"doc\":\"Doc\","
                + "\"a_very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"very_important\":[\"important_stuff_1\","
                + "\"important_stuff_2\"],\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"dbl\","
                + "\"type\":\"double\","
                + "\"doc\":\"Doc\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"]"
                + ",\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"isTrue\","
                + "\"type\":\"boolean\","
                + "\"doc\":\"Doc\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"arrayOfStrings\","
                + "\"type\":"
                  + "{"
                  + "\"type\":\"array\","
                  + "\"items\":\"string\""
                  + "},"
                + "\"doc\":\"Doc\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"min\","
                + "\"type\":"
                + "{"
                  + "\"name\":\"Amount\","
                  + "\"type\":\"record\","
                  + "\"fields\":["
                    + "{"
                      + "\"name\":\"currencyCode\","
                      + "\"type\":\"string\","
                      + "\"doc\":\"Currency code v$\""
                    + "},"
                    + "{"
                      + "\"name\":\"amount\","
                      + "\"type\":\"string\","
                      + "\"doc\":\"The amount of money as a real number string, See https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html#BigDecimal-java.lang.String-\""
                    + "}"
                  + "],"
                  + "\"aliases\":["
                    + "\"banana.MoneyAmount\",\"ms14.MoneyAmount\",\"vs14.MoneyAmount\",\"vs14.NoNamespace\"],"
                  + "\"doc\":\"Represents an amount of money\""
                + "},"
                + "\"doc\":\"Minimum value\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"mapOfStrings\","
                 + "\"type\":{"
                  + "\"type\":\"map\","
                  + "\"values\":\"string\""
                 + "},"
                + "\"doc\":\"Doc\","
                + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
                + "\"another_non_important_json\":\"decreases apple count by 1.\","
                + "\"not_important_stuff\":\"an apple a day!\""
              + "},"
              + "{"
                + "\"name\":\"fixedType\","
              + "\"type\":"
              + "{"
                + "\"name\":\"RandomFixedName\","
                + "\"type\":\"fixed\","
                + "\"size\":16,"
                + "\"doc\":\"Doc\""
              + "},"
              + "\"doc\":\"Doc\","
              + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
              + "\"another_non_important_json\":\"decreases apple count by 1.\","
              + "\"not_important_stuff\":\"an apple a day!\""
            + "},"
            + "{"
              + "\"name\":\"unionOfArray\","
              + "\"type\":"
              + "[\"null\",{"
              + "\"type\":\"array\","
              + "\"items\":\"string\""
            + "}],"
            + "\"doc\":\"Doc\","
            + "\"very_important\":[\"important_stuff_1\",\"important_stuff_2\"],"
            + "\"another_non_important_json\":\"decreases apple count by 1.\","
            + "\"not_important_stuff\":\"an apple a day!\""
            + "}],"
            + "\"record_level_important_json\":\"Really important space exploration plans\","
            + "\"a_record_level_important_json\":\"Really important space exploration plans\","
            + "\"a_record_level_junk_json\":\"Non important text. Put me first\","
            + "\"record_level_junk_json\":\"Non important text\""
        + "}";

    Schema schema = Schema.parse(TestUtil.load("OrderTester.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, config,
        Arrays.asList(
            // non sorted order, output expect in the same order as plugins
            new SchemaLevelPlugin("record_level_important_json"),
            new SchemaLevelPlugin("a_record_level_important_json"),
            // sorted order, output expect in the same order as plugins
            new FieldLevelPlugin("a_very_important"),
            new FieldLevelPlugin("very_important")
        ));

    Assert.assertEquals(str, expectedForm);
  }

  @Test
  public void testCanonicalBroadWithSchemaAndFieldPlugin() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));
    Schema canonicalizedSchema = Schema.parse(str);

    //Copies default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(0).hasDefaultValue());
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).defaultVal(), "A");
    Assert.assertEquals(canonicalizedSchema.getFields().get(3).defaultVal().toString(), "{innerLongField=420}");

    // Copies top level record alias, fully qualified and sorted
    Assert.assertTrue(canonicalizedSchema.getAliases().toString().equals("[com.acme.a_record_alias, com.acme.record_alias]"));

    // Copies field schema aliases, and is now sorted and fully qualified
    Assert.assertTrue(canonicalizedSchema.getFields().get(2).schema().getAliases().toString().equals("[com.acme.Afield_type_alias, com.acme.field_type_alias]"));

    // Copies field level aliases, and is now sorted
    Assert.assertTrue(canonicalizedSchema.getFields().get(3).aliases().toString().equals("[afield_alias, field_alias]"));

    // Copies Schema level property
    Assert.assertEquals(canonicalizedSchema.getProp("record_level_important_json"),
        "Really important space exploration plans");

    // Copies Field level property
    Assert.assertEquals(canonicalizedSchema.getField("recordField")
        .schema()
        .getField("innerLongField")
        .getObjectProp("very_important").toString(), "[important_stuff_1, important_stuff_2]");
  }

  @Test
  public void testFingerprintBareBonesCanonicalForm() throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String canonicalSchemaStr = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
        Collections.EMPTY_LIST);
    String canonicalSchemaStrWithPlugins = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));


    byte[] canonicalStrictMD5FP = AvroUtilSchemaNormalization.parsingFingerprint(AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, schema,
        AvscGenerationConfig.CANONICAL_ONELINE, Collections.EMPTY_LIST);

    byte[] canonicalStrictXXFpFromParsedSchema = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, canonicalSchemaStr.getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(canonicalStrictMD5FP, canonicalStrictXXFpFromParsedSchema);

    byte[] canonicalStrictMd5FPFromParsedSchemaWithPlugins = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, canonicalSchemaStrWithPlugins.getBytes(StandardCharsets.UTF_8));
    Assert.assertNotEquals(canonicalStrictMD5FP, canonicalStrictMd5FPFromParsedSchemaWithPlugins);
  }


  /***
   * Test broad canonical form with Schema and Field plugins.
   * Schema 1 = Schema 2 + different docs + aliases
   *
   * * Broad canonical form should not be equal
   * * Docs should not be retained
   * * avro.java.string property is retained
   */
  @Test
  public void testBroadCanonicalFormSchemasWithPlugins() throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilarNoAliases = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    String canonicalSchemaStrWithPluginsBroad =
        AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
            Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                new FieldLevelPlugin("very_important")));

    String canonicalSimilarSchemaStrWithPluginsBroad =
        AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilarNoAliases, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
            Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                new FieldLevelPlugin("very_important")));


    Schema schemaFromCanonicalSchema = Schema.parse(canonicalSchemaStrWithPluginsBroad);
    Schema schemaFromCanonicalSimilarSchema = Schema.parse(canonicalSimilarSchemaStrWithPluginsBroad);

    // broad canonical form should NOT be equal, no aliases in schema 2
    Assert.assertNotEquals(canonicalSchemaStrWithPluginsBroad, canonicalSimilarSchemaStrWithPluginsBroad);

    // Top level doc is not retained
    Assert.assertNull(schemaFromCanonicalSchema.getDoc());
    Assert.assertNull(schemaFromCanonicalSimilarSchema.getDoc());

    // Assert it copied avro.java.string property
    Assert.assertEquals(schemaFromCanonicalSchema.getField("strTest").schema().getProp("avro.java.string"), "String");
  }

  /***
   * Test narrow canonical form with Schema and Field plugins.
   * Schema 1 = Schema 2 + different docs + aliases
   *
   * * Narrow canonical form should be equal
   * * Parsing fingerprint should be equal
   * * Docs should not be retained
   * * avro.java.string property is retained
   * * Aliases are not retained
   * * Schema and Field json properties handled by plugins were retained
   */
  @Test
  public void testNarrowCanonicalFormCompareTwoDifferentButIdenticalSchemasWithPlugins()
      throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilarNoAliases = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    String canonicalSchemaStrWithPlugins =
        AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
            Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                new FieldLevelPlugin("very_important")));

    String canonicalSimilarSchemaStrWithPlugins =
        AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilarNoAliases, AvscGenerationConfig.CANONICAL_ONELINE,
            Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                new FieldLevelPlugin("very_important")));

    // fingerprint with plugins
    for(AvroUtilSchemaNormalization.FingerprintingAlgo algo : AvroUtilSchemaNormalization.FingerprintingAlgo.values()) {
      byte[] schemaWithPluginsFingerprint = AvroUtilSchemaNormalization.parsingFingerprint(algo, schema,
          AvscGenerationConfig.CANONICAL_ONELINE,
          Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));
      byte[] similarSchemaWithPluginsFingerprint =
          AvroUtilSchemaNormalization.parsingFingerprint(algo, schemaSimilarNoAliases, AvscGenerationConfig.CANONICAL_ONELINE,
              Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                  new FieldLevelPlugin("very_important")));

      Assert.assertEquals(schemaWithPluginsFingerprint, similarSchemaWithPluginsFingerprint);
    }


    Schema schemaFromCanonicalSchema = Schema.parse(canonicalSchemaStrWithPlugins);
    Schema schemaFromCanonicalSimilarSchema = Schema.parse(canonicalSimilarSchemaStrWithPlugins);

    // narrow canonical form should be equal, no aliases in schema 2
    Assert.assertEquals(canonicalSchemaStrWithPlugins, canonicalSimilarSchemaStrWithPlugins);


    // Top level doc is not retained
    Assert.assertNull(schemaFromCanonicalSchema.getDoc());
    Assert.assertNull(schemaFromCanonicalSimilarSchema.getDoc());

    // Assert it copied avro.java.string property
    Assert.assertEquals(schemaFromCanonicalSchema.getField("strTest").schema().getProp("avro.java.string"), "String");

    // Assert, original schema has alias but were not retained
    Assert.assertFalse(schema.getField("enumField").schema().getAliases().isEmpty());
    Assert.assertTrue(schemaFromCanonicalSchema.getField("enumField").schema().getAliases().isEmpty());

    // Assert Schema extra json were present
    Assert.assertEquals(schema.getProp("record_level_important_json"), "Really important space exploration plans");
    Assert.assertEquals(schema.getProp("record_level_junk_json"), "Non important text");
    // AND retained
    Assert.assertEquals(schemaFromCanonicalSchema.getProp("record_level_important_json"), "Really important space exploration plans");
    // But prop not handled by plugins was not retained
    Assert.assertNull(schemaFromCanonicalSchema.getProp("record_level_junk_json"));
  }

  /***
   * Test narrow canonical form fingerprint with NO plugins.
   * Schema 1 = Schema 2 + different docs + aliases
   * * Fingerprint for all supported algorithms should match
   * * Parsing fingerprint should be equal
   */
  @Test
  public void testNarrowCanonicalFingerprintsNoPlugins() throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilarNoAliases = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    String canonicalSchemaStr =
        AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);

    String canonicalSimilarSchemaStr =
        AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilarNoAliases, AvscGenerationConfig.CANONICAL_ONELINE,
            null);

    // fingerprint with plugins
    for (AvroUtilSchemaNormalization.FingerprintingAlgo algo : AvroUtilSchemaNormalization.FingerprintingAlgo.values()) {
      byte[] parsingSchemaFingerprint =
          AvroUtilSchemaNormalization.parsingFingerprint(algo, schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
      byte[] parsingSimilarSchemaFingerprint = AvroUtilSchemaNormalization.parsingFingerprint(algo, schemaSimilarNoAliases,
          AvscGenerationConfig.CANONICAL_ONELINE, null);

      byte[] schemaFingerprintFromAvsc = AvroUtilSchemaNormalization.fingerprint(algo, canonicalSchemaStr.getBytes(StandardCharsets.UTF_8));
      byte[] similarSchemaFingerprintFromAvsc = AvroUtilSchemaNormalization.fingerprint(algo, canonicalSimilarSchemaStr.getBytes(StandardCharsets.UTF_8));

      // Parsing fingerprint
      Assert.assertEquals(parsingSchemaFingerprint, parsingSimilarSchemaFingerprint);

      // Fingerprint from canonical avsc
      Assert.assertEquals(schemaFingerprintFromAvsc, similarSchemaFingerprintFromAvsc);
    }
  }

  /***
   * Test broad canonical form fingerprint with NO plugins.
   * Schema 1 = Schema 2 + different docs + aliases
   * * Fingerprint for all supported algorithms should NOT match
   * * Parsing fingerprint should be NOT equal
   */
  @Test
  public void testBroadCanonicalSchemaFingerprintsNoPlugins() throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilarNoAliases = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    String canonicalSchemaStr =
        AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
            Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                new FieldLevelPlugin("very_important")));

    String canonicalSimilarSchemaStr = AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilarNoAliases,
        AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

    // fingerprint with plugins, not equal
    for (AvroUtilSchemaNormalization.FingerprintingAlgo algo : AvroUtilSchemaNormalization.FingerprintingAlgo.values()) {
      byte[] parsingSchemaFingerprint =
          AvroUtilSchemaNormalization.parsingFingerprint(algo, schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
              Arrays.asList(new SchemaLevelPlugin("record_level_important_json"),
                  new FieldLevelPlugin("very_important")));
      byte[] parsingSimilarSchemaFingerprint = AvroUtilSchemaNormalization.parsingFingerprint(algo, schemaSimilarNoAliases,
          AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
          Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

      byte[] schemaFingerprintFromAvsc = AvroUtilSchemaNormalization.fingerprint(algo, canonicalSchemaStr.getBytes(StandardCharsets.UTF_8));
      byte[] similarSchemaFingerprintFromAvsc = AvroUtilSchemaNormalization.fingerprint(algo, canonicalSimilarSchemaStr.getBytes(StandardCharsets.UTF_8));

      // Parsing fingerprint
      Assert.assertNotEquals(parsingSchemaFingerprint, parsingSimilarSchemaFingerprint);

      // Fingerprint from canonical avsc
      Assert.assertNotEquals(schemaFingerprintFromAvsc, similarSchemaFingerprintFromAvsc);
    }

  }

  @Test
  public void testNarrowCanonicalSchemaFingerprintsWithPlugins() throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilarNoAliases = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    String canonicalSchemaStr =
        AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);

    String canonicalSimilarSchemaStr =
        AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilarNoAliases, AvscGenerationConfig.CANONICAL_ONELINE,
            null);

    // fingerprint with plugins
    for (AvroUtilSchemaNormalization.FingerprintingAlgo algo : AvroUtilSchemaNormalization.FingerprintingAlgo.values()) {
      byte[] schemaFingerprint =
          AvroUtilSchemaNormalization.parsingFingerprint(algo, schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
      byte[] similarSchemaFingerprint = AvroUtilSchemaNormalization.parsingFingerprint(algo, schemaSimilarNoAliases,
          AvscGenerationConfig.CANONICAL_ONELINE, null);

      Assert.assertEquals(schemaFingerprint, similarSchemaFingerprint);
    }

    Schema schemaFromCanonicalSchema = Schema.parse(canonicalSchemaStr);
    Schema schemaFromCanonicalSimilarSchema = Schema.parse(canonicalSimilarSchemaStr);

    // narrow canonical form should be equal, no aliases in schema 2
    Assert.assertEquals(canonicalSchemaStr, canonicalSimilarSchemaStr);

    // Top level doc is not retained
    Assert.assertNull(schemaFromCanonicalSchema.getDoc());
    Assert.assertNull(schemaFromCanonicalSimilarSchema.getDoc());

    // Assert it copied avro.java.string property
    Assert.assertEquals(schemaFromCanonicalSchema.getField("strTest").schema().getProp("avro.java.string"), "String");

    // Assert, original schema has alias but were not retained
    Assert.assertFalse(schema.getField("enumField").schema().getAliases().isEmpty());
    Assert.assertTrue(schemaFromCanonicalSchema.getField("enumField").schema().getAliases().isEmpty());

    // Assert Field extra json were present
    Assert.assertEquals(schema.getField("nullWithoutDefault").getProp("not_important_stuff"), "an apple a day!");
    Assert.assertEquals(schema.getField("nullWithoutDefault").getProp("another_non_important_json"),
        "decreases apple count by 1.");
    // but not retained
    Assert.assertNull(schemaFromCanonicalSchema.getField("nullWithoutDefault").getProp("not_important_stuff"));
    Assert.assertNull(schemaFromCanonicalSchema.getField("nullWithoutDefault").getProp("another_non_important_json"));

    // Assert Schema extra json were present
    Assert.assertEquals(schema.getProp("record_level_important_json"), "Really important space exploration plans");
    // but not retained
    Assert.assertNull(schemaFromCanonicalSchema.getProp("record_level_important_json"));
  }

  @Test
  public void testBareBonesCanonicalFormCompareTwoDifferentButIdenticalSchemasNoPlugins()
      throws IOException, NoSuchAlgorithmException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    Schema schemaSimilar = Schema.parse(TestUtil.load("Record1DifferentDocsNoAliases.avsc"));

    // Narrow canonical form, schema, with and without plugins
    String canonicalSchemaStr = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
        Collections.EMPTY_LIST);
    String canonicalSchemaStrWithPlugins = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

    // Narrow canonical form, schema-similar, with and without plugins
    String canonicalSimilarSchemaStr = AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilar, AvscGenerationConfig.CANONICAL_ONELINE,
        Collections.EMPTY_LIST);
    String canonicalSimilarSchemaStrWithPlugins = AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilar, AvscGenerationConfig.CANONICAL_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

    // Canonical form broad, schema, with And without plugins
    String canonicalSchemaStrBroad = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Collections.EMPTY_LIST);
    String canonicalSchemaStrWithPluginsBroad = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

    // Canonical form broad, schema-similar, with And without plugins
    String canonicalSimilarSchemaStrBroad = AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilar, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Collections.EMPTY_LIST);
    String canonicalSimilarSchemaStrWithPluginsBroad = AvroUtilSchemaNormalization.getCanonicalForm(schemaSimilar, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Arrays.asList(new SchemaLevelPlugin("record_level_important_json"), new FieldLevelPlugin("very_important")));

    // Test
    // Narrow canonical should be equal
    Assert.assertEquals(canonicalSchemaStr, canonicalSimilarSchemaStr);
    // Narrow canonical with plugins
    Assert.assertEquals(canonicalSchemaStrWithPlugins, canonicalSimilarSchemaStrWithPlugins);

    // Broad canonical Should not be equal
    Assert.assertNotEquals(canonicalSchemaStrBroad, canonicalSimilarSchemaStrBroad);
    // Broad canonical with plugins should not be equal
    Assert.assertNotEquals(canonicalSchemaStrWithPluginsBroad, canonicalSimilarSchemaStrWithPluginsBroad);

    // fingerprints
    // schema, narrow with and without plugins
    byte[] canonicalStrictXXFp = AvroUtilSchemaNormalization.parsingFingerprint(AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, schema,
        AvscGenerationConfig.CANONICAL_ONELINE, Collections.EMPTY_LIST);


    // fingerprints from parsed schemas - narrow - with and without plugins
    byte[] canonicalStrictXXFpFromParsedSchemaWithPlugins = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, canonicalSchemaStrWithPlugins.getBytes(StandardCharsets.UTF_8));


    // fingerprints from parsed schemas - broad

    // schema-similar, narrow with and without plugins
    byte[] canonicalStrictXXFpForSimilarSchema = AvroUtilSchemaNormalization.parsingFingerprint(AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, schemaSimilar,
        AvscGenerationConfig.CANONICAL_ONELINE, Collections.EMPTY_LIST);
    byte[] canonicalStrictXXFpFromParsedSimilarSchemaWithPlugins = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.XX_128, canonicalSimilarSchemaStrWithPlugins.getBytes(StandardCharsets.UTF_8));

    //Equal canonical schema
    Assert.assertEquals(canonicalSchemaStr, canonicalSimilarSchemaStr);
    //Equal canonical schema with plugins
    Assert.assertEquals(canonicalSchemaStrWithPlugins, canonicalSimilarSchemaStrWithPlugins);
    // Same fingerprint
    Assert.assertEquals(canonicalStrictXXFp, canonicalStrictXXFpForSimilarSchema);
    //Same fingerprint with plugins
    Assert.assertEquals(canonicalStrictXXFpFromParsedSchemaWithPlugins, canonicalStrictXXFpFromParsedSimilarSchemaWithPlugins);

    //Different wider canonical form
    Assert.assertNotEquals(canonicalSimilarSchemaStrBroad, canonicalSimilarSchemaStrWithPluginsBroad);
  }

  // Record without namespace -> Canonical form with empty namespace
  @Test
  public void testBlankNamespaceRecord() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("RecordNoNamespace.avsc"));
    String canonicalForm = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
    String canonicalFormBroad = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, null);
    Assert.assertTrue(canonicalForm.startsWith("{\"name\":\"RecordNoNamespace\",\"namespace\":\"\""));
    Assert.assertTrue(canonicalFormBroad.startsWith("{\"name\":\"RecordNoNamespace\",\"namespace\":\"\""));
  }


  // Record with empty space namespace -> Canonical form with empty namespace
  @Test
  public void testEmtpySpaceNamespaceRecord() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("RecordEmptySpaceNamespace.avsc"));
    String canonicalForm = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
    String canonicalFormBroad = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, null);
    Assert.assertTrue(canonicalForm.startsWith("{\"name\":\"RecordEmptySpaceNamespace\",\"namespace\":\"\""));
    Assert.assertTrue(canonicalFormBroad.startsWith("{\"name\":\"RecordEmptySpaceNamespace\",\"namespace\":\"\""));
  }

  // Inner namedTypes should contain explicit namespaces
  @Test
  public void explicitNamespacesTest() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("RecordWithInnerTypes.avsc"));
    String canonicalForm = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE, null);
    String canonicalFormBroad = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, null);
    Assert.assertTrue(canonicalForm.startsWith("{\"name\":\"RecordWithInnerTypes\",\"namespace\":\"com.acme\""));
    Assert.assertTrue(canonicalForm.contains("{\"name\":\"EnumForHasDefaults\",\"namespace\":\"com.acme\""));
    Assert.assertTrue(canonicalForm.contains("{\"name\":\"SpecialRecordName\",\"namespace\":\"com.my.own.special\""));
    Assert.assertTrue(canonicalFormBroad.startsWith("{\"name\":\"RecordWithInnerTypes\",\"namespace\":\"com.acme\""));
    Assert.assertTrue(canonicalFormBroad.contains("{\"name\":\"EnumForHasDefaults\",\"namespace\":\"com.acme\""));
    Assert.assertTrue(canonicalFormBroad.contains("{\"name\":\"SpecialRecordName\",\"namespace\":\"com.my.own.special\""));

  }

  private class SchemaLevelPlugin extends AvscWriterPlugin {
    public SchemaLevelPlugin(String prop_name) {
       super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson2JsonGeneratorWrapper gen){
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, JacksonUtils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    @Override
    public String execute(Schema schema, JsonGeneratorWrapper gen) {
      if (schema.hasProps()) {
        String prop = schema.getProp(PROP_NAME);
        if(prop == null) {
          return null;
        }
        writeProp(PROP_NAME, prop, (Jackson2JsonGeneratorWrapper) gen);
      }
      return PROP_NAME;
    }
  }

  private class FieldLevelPlugin extends AvscWriterPlugin {

    public FieldLevelPlugin(String prop_name) {
      super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson2JsonGeneratorWrapper gen){
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, JacksonUtils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String execute(Schema.Field field, JsonGeneratorWrapper gen) {
      if (field.hasProps()) {
        Object prop = field.getObjectProp(PROP_NAME);
        if(prop == null) {
          return null;
        }
        writeProp(PROP_NAME, prop, (Jackson2JsonGeneratorWrapper) gen);
      }
      return PROP_NAME;
    }
  }

}
