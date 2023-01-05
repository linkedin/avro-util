/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class ConfigurableSchemaComparatorTest {

  @Test
  public void showOldAvroCantCompareNonStrings() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"schemaIntProp\": 1,\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"fieldIntProp\": 2\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"schemaIntProp\": 3,\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"fieldIntProp\": 4\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    Schema a = Schema.parse(avscA);
    Schema b = Schema.parse(avscB);
    switch (runtimeAvroVersion) {
      case AVRO_1_4:
      case AVRO_1_5:
      case AVRO_1_6:
        Assert.assertEquals(a, b);
        break;
      case AVRO_1_7: //capability is added in 1.7.3, so in the "middle" of 1.7
        break;
      default:
        Assert.assertNotEquals(a, b); //int props kick in
    }
  }

  @Test
  public void testExceptionOnTryingToCompareNonStringsUnderOldAvro() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    String avscA = "\"string\"";
    String avscB = "\"int\"";
    Schema a = Schema.parse(avscA);
    Schema b = Schema.parse(avscB);
    SchemaComparisonConfiguration config = new SchemaComparisonConfiguration(
        false,
        true, //boom
        false,
        true,
        true,
        true,
        Collections.emptySet()
    );
    switch (runtimeAvroVersion) {
      case AVRO_1_4:
      case AVRO_1_5:
      case AVRO_1_6:
        try {
          ConfigurableSchemaComparator.equals(a, b, config);
          Assert.fail("expected to throw for " + runtimeAvroVersion);
        } catch (IllegalArgumentException expected) {
          //expected
        }
        break;
      case AVRO_1_7: //capability is added in 1.7.3, so in the "middle" of 1.7
        break;
      default:
        Assert.assertFalse(ConfigurableSchemaComparator.equals(a, b, config));
    }
  }

  @Test
  public void testCompareOnlyStringProps() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"schemaStrProp\": \"val1\",\n"
        + "  \"schemaIntProp\": 1,\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"fieldStrProp\": \"val2\",\n"
        + "      \"fieldIntProp\": 2\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"schemaStrProp\": \"val1\",\n"
        + "  \"schemaIntProp\": 3,\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f\",\n"
        + "      \"type\": \"string\",\n"
        + "      \"fieldStrProp\": \"val2\",\n"
        + "      \"fieldIntProp\": 4\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    Schema a = Schema.parse(avscA);
    Schema b = Schema.parse(avscB);
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, SchemaComparisonConfiguration.PRE_1_7_3));
    if (runtimeAvroVersion.laterThan(AvroVersion.AVRO_1_7)) {
      //complex props are only supported by avro >= 1.7.3
      Assert.assertFalse(ConfigurableSchemaComparator.equals(a, b, SchemaComparisonConfiguration.STRICT));
    }
  }

  @Test
  public void testLooseNumericDefaults() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_8)) {
      throw new SkipException("strict parsing doesnt work under " + runtimeAvroVersion);
    }
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"default\": 1.0\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"f2\",\n"
        + "      \"type\": \"double\",\n"
        + "      \"default\": 2\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"default\": 1\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"f2\",\n"
        + "      \"type\": \"double\",\n"
        + "      \"default\": 2.0\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    Schema a = AvroCompatibilityHelper.parse(avscA, SchemaParseConfiguration.LOOSE_NUMERICS, null).getMainSchema();
    Schema b = AvroCompatibilityHelper.parse(avscB, SchemaParseConfiguration.LOOSE_NUMERICS, null).getMainSchema();
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, SchemaComparisonConfiguration.LOOSE_NUMERICS));
    Assert.assertFalse(ConfigurableSchemaComparator.equals(a, b, SchemaComparisonConfiguration.STRICT));
  }

  @Test
  public void testCompareFieldAliases() throws Exception {
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"aliases\": [\"f2\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscC = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"aliases\": [\"f2\", \"f3\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    Schema a = AvroCompatibilityHelper.parse(avscA, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema b = AvroCompatibilityHelper.parse(avscB, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema c = AvroCompatibilityHelper.parse(avscC, SchemaParseConfiguration.STRICT, null).getMainSchema();

    SchemaComparisonConfiguration strict = SchemaComparisonConfiguration.STRICT
        .compareNonStringJsonProps(false); //required to work under old avro

    SchemaComparisonConfiguration ignoreAliases = strict
        .compareAliases(false); //purpose of this test

    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, c, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(b, c, ignoreAliases));

    Assert.assertFalse(ConfigurableSchemaComparator.equals(a, b, strict));
    Assert.assertFalse(ConfigurableSchemaComparator.equals(a, c, strict));
    Assert.assertFalse(ConfigurableSchemaComparator.equals(b, c, strict));
  }

  @Test
  public void testJsonPropNamesToIgnore() {
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"myProp\": \"f2\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"myProp\": \"f2\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String avscC = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"unknownProp\": \"f2\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\",\n"
        + "      \"myProp\": \"f2\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Schema a = AvroCompatibilityHelper.parse(avscA, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema b = AvroCompatibilityHelper.parse(avscB, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema c = AvroCompatibilityHelper.parse(avscC, SchemaParseConfiguration.STRICT, null).getMainSchema();

    SchemaComparisonConfiguration ignoreMyProp = SchemaComparisonConfiguration.STRICT
        .jsonPropNamesToIgnore(Collections.singleton("myProp"))
        .compareNonStringJsonProps(false); //required to work under old avro

    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, ignoreMyProp));
    Assert.assertFalse(ConfigurableSchemaComparator.equals(b, c, ignoreMyProp));
  }

  @Test
  public void testCompareSchemaAliases() throws Exception {
    String avscA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"namespace\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"aliases\": [\"OldBob\"],\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscB = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"namespace\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"aliases\": [\"namespace.OldBob\"],\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscC = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"namespace\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"aliases\": [\"oldspace.OldBob\"],\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    String avscD = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"namespace\",\n"
        + "  \"name\": \"Bob\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"f1\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Schema a = AvroCompatibilityHelper.parse(avscA, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema b = AvroCompatibilityHelper.parse(avscB, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema c = AvroCompatibilityHelper.parse(avscC, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema d = AvroCompatibilityHelper.parse(avscD, SchemaParseConfiguration.STRICT, null).getMainSchema();

    SchemaComparisonConfiguration strict = SchemaComparisonConfiguration.STRICT
        .compareNonStringJsonProps(false); //required to work under old avro

    SchemaComparisonConfiguration ignoreAliases = strict
        .compareAliases(false); //purpose of this test

    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, c, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, d, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(b, c, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(b, d, ignoreAliases));
    Assert.assertTrue(ConfigurableSchemaComparator.equals(c, d, ignoreAliases));

    //type aliases inherit namespace, see https://avro.apache.org/docs/current/spec.html#Aliases
    Assert.assertTrue(ConfigurableSchemaComparator.equals(a, b, strict));

    Assert.assertFalse(ConfigurableSchemaComparator.equals(a, c, strict));
    Assert.assertFalse(ConfigurableSchemaComparator.equals(a, d, strict));
  }
}
