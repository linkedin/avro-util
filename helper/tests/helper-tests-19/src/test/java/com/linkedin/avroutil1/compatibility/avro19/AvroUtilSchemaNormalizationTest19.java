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
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import com.linkedin.avroutil1.normalization.AvroUtilSchemaNormalization;
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

    Assert.assertTrue(ConfigurableSchemaComparator.equals(canonicalizedSchema, schemaFromAvroNormalizer, SchemaComparisonConfiguration.STRICT));

    // Non null
    Assert.assertNotNull(canonicalizedSchema);

    // No docs
    Assert.assertNull(canonicalizedSchema.getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(0).schema().getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(1).schema().getDoc());
    Assert.assertNull(canonicalizedSchema.getFields().get(2).schema().getDoc());

    // No default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(0).hasDefaultValue());
    Assert.assertFalse(canonicalizedSchema.getFields().get(1).hasDefaultValue());
    Assert.assertFalse(canonicalizedSchema.getFields().get(2).hasDefaultValue());

    // Doesn't copy top level record alias
    Assert.assertFalse(canonicalizedSchema.getAliases().contains("com.acme.record_alias"));

    // Doesn't copy field schema aliases
    Assert.assertFalse(canonicalizedSchema.getFields().get(1).schema().getAliases().contains("com.acme.field_type_alias"));

    // Doesn't copy field level aliases
    Assert.assertFalse(canonicalizedSchema.getFields().get(2).aliases().contains("field_alias"));

    // Doesn't copy specified junk json/extra json props from Field
    Assert.assertNotNull(schema.getFields().get(2).schema().getFields().get(0).getObjectProp("very_important"));
    Assert.assertNull(canonicalizedSchema.getFields().get(2).schema().getFields().get(0).getObjectProp("very_important"));

    // Ignores other junk json/extra json props from field level
    Assert.assertNotNull(schema.getFields().get(0).getProp("not_important_stuff"));
    Assert.assertNull(canonicalizedSchema.getFields().get(0).getProp("not_important_stuff"));

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
    Assert.assertEquals(canonicalizedSchema.getFields().get(1).defaultVal(), "A");
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).defaultVal().toString(), "{innerLongField=420}");

    // Copies top level record alias, fully qualified and sorted
    Assert.assertTrue(canonicalizedSchema.getAliases().toString().equals("[com.acme.a_record_alias, com.acme.record_alias]"));

    // Copies field schema aliases, and is now sorted and fully qualified
    Assert.assertTrue(canonicalizedSchema.getFields().get(1).schema().getAliases().toString().equals("[com.acme.Afield_type_alias, com.acme.field_type_alias]"));

    // Copies field level aliases, and is now sorted
    Assert.assertTrue(canonicalizedSchema.getFields().get(2).aliases().toString().equals("[afield_alias, field_alias]"));
  }

  @Test
  public void testCanonicalStrictWithNonSpecificJsonIncluded() throws IOException {
    AvscGenerationConfig config = new AvscGenerationConfig(
        false, false, false, Optional.of(Boolean.TRUE), false, false,
        false, false, true, false);

    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, config, null);
    Schema canonicalizedSchema = Schema.parse(str);

    // copied all props, sorted by key
    Assert.assertEquals(new TreeMap(schema.getObjectProps()), canonicalizedSchema.getObjectProps());

    // for fields
    for(Schema.Field field: schema.getFields()) {
      Schema.Field canonicalizedField = canonicalizedSchema.getField(field.name());
      if(field.hasProps()) {
        Assert.assertEquals(new TreeMap<>(field.getObjectProps()), canonicalizedField.getObjectProps());
      }

      // and for field schemas
      if(field.schema().hasProps()) {
        Assert.assertEquals(new TreeMap<>(field.schema().getObjectProps()), canonicalizedField.schema().getObjectProps());
      }
    }

  }

  @Test
  public void testCanonicalBroadWithSchemaAndFieldPlugin() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("Record1.avsc"));
    String str = AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_BROAD_ONELINE,
        Arrays.asList(new SchemaLevelPlugin(), new FieldLevelPlugin()));
    Schema canonicalizedSchema = Schema.parse(str);

    //Copies default value
    Assert.assertFalse(canonicalizedSchema.getFields().get(0).hasDefaultValue());
    Assert.assertEquals(canonicalizedSchema.getFields().get(1).defaultVal(), "A");
    Assert.assertEquals(canonicalizedSchema.getFields().get(2).defaultVal().toString(), "{innerLongField=420}");

    // Copies top level record alias, fully qualified and sorted
    Assert.assertTrue(canonicalizedSchema.getAliases().toString().equals("[com.acme.a_record_alias, com.acme.record_alias]"));

    // Copies field schema aliases, and is now sorted and fully qualified
    Assert.assertTrue(canonicalizedSchema.getFields().get(1).schema().getAliases().toString().equals("[com.acme.Afield_type_alias, com.acme.field_type_alias]"));

    // Copies field level aliases, and is now sorted
    Assert.assertTrue(canonicalizedSchema.getFields().get(2).aliases().toString().equals("[afield_alias, field_alias]"));

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
        Arrays.asList(new SchemaLevelPlugin(), new FieldLevelPlugin()));


    byte[] canonicalStrictMD5FP = AvroUtilSchemaNormalization.parsingFingerprint(AvroUtilSchemaNormalization.FingerprintingAlgo.MD5_128, schema,
        AvscGenerationConfig.CANONICAL_ONELINE, Collections.EMPTY_LIST);

    byte[] canonicalStrictMd5FPFromParsedSchema = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.MD5_128, canonicalSchemaStr.getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(canonicalStrictMD5FP, canonicalStrictMd5FPFromParsedSchema);

    byte[] canonicalStrictMd5FPFromParsedSchemaWithPlugins = AvroUtilSchemaNormalization.fingerprint(
        AvroUtilSchemaNormalization.FingerprintingAlgo.MD5_128, canonicalSchemaStrWithPlugins.getBytes(StandardCharsets.UTF_8));
    Assert.assertNotEquals(canonicalStrictMD5FP, canonicalStrictMd5FPFromParsedSchemaWithPlugins);

  }


  private class SchemaLevelPlugin implements AvscWriterPlugin {

    private final String PROP_NAME = "record_level_important_json";

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

  private class FieldLevelPlugin implements AvscWriterPlugin {

    private final String PROP_NAME = "very_important";

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
