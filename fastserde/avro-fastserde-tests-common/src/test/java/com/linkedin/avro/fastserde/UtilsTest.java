package com.linkedin.avro.fastserde;

import java.util.Arrays;

import javax.lang.model.SourceVersion;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.generated.avro.FloatWithDefaultNaN;


public class UtilsTest {

  @Test (groups = "deserializationTest")
  public void testGenerateSourcePathFromPackageName() {
    Assert.assertEquals(Utils.generateSourcePathFromPackageName("com.linkedin.avro"), Utils.fixSeparatorsToMatchOS("/com/linkedin/avro/"));
  }

  @Test
  void testFingerprintOfSchemaWithDefaultNaN() {
    // expect no exception is thrown
    Utils.getSchemaFingerprint(FloatWithDefaultNaN.SCHEMA$);
  }

  @DataProvider
  static Object[][] createValidJavaIdentifierTestCases() {
    String[] validOrAlmostValidJavaIdentifiers = {
            "goodOne",
            "fine",
            "break",
            "class",
            "field_name",
            "var",
            "varName",
            "    will be   cleared  as weLL  %$^$&^*(*)!@#@#${} "
    };

    return Arrays.stream(validOrAlmostValidJavaIdentifiers)
            .map(str -> new Object[]{str})
            .toArray(Object[][]::new);
  }

  @Test(dataProvider = "createValidJavaIdentifierTestCases")
  void shouldGenerateValidJavaIdentifier(String javaIdentifierCandidate) {
    // when
    String validJavaIdentifier = Utils.toValidJavaIdentifier(javaIdentifierCandidate);

    // when
    Assert.assertTrue(SourceVersion.isIdentifier(validJavaIdentifier));
  }

  /*-----------------------------------------*/

  @DataProvider
  static Object[][] createInvalidJavaIdentifierTestCases() {
    String[] invalidJavaIdentifiers = {
            null,
            "",
            " ",
            "\n\t\r",
            "\n   \t   \r "
    };

    return Arrays.stream(invalidJavaIdentifiers)
            .map(str -> new Object[]{str})
            .toArray(Object[][]::new);
  }

  @Test(dataProvider = "createInvalidJavaIdentifierTestCases", expectedExceptions = IllegalArgumentException.class)
  void shouldFailGeneratingValidJavaIdentifier(String invalidProposal) {
    // NPE expected
    Utils.toValidJavaIdentifier(invalidProposal);
  }

  @Test
  void testGetTruncateSchemaForWarningNull() {
    Schema schema = null;
    Assert.assertEquals("null", Utils.getTruncateSchemaForWarning(schema));
  }

  @Test
  void testGetTruncateSchemaForWarningSmall() {
    String schemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "   {\"name\": \"name\", \"type\": \"string\"}"
            + "]"
            + "}";
    Schema schema = Schema.parse(schemaJson);
    Assert.assertTrue(schema.toString().length() <= Utils.MAX_SCHEMA_LENGTH_IN_WARNING);
    Assert.assertEquals(schema.toString(), Utils.getTruncateSchemaForWarning(schema));
  }

  @Test
  void testGetTruncateSchemaForWarningLarge() {
    String schemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"namespace\": \"com.example.avro\","
            + "\"fields\": ["
            + "   {\"name\": \"name\", \"type\": \"string\"},"
            + "   {\"name\": \"age\", \"type\": \"int\"},"
            + "   {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null}"
            + "]"
            + "}";
    Schema schema = Schema.parse(schemaJson);
    Assert.assertTrue(schema.toString().length() > Utils.MAX_SCHEMA_LENGTH_IN_WARNING);
    String truncatedSchema = Utils.getTruncateSchemaForWarning(schema);
    Assert.assertEquals(truncatedSchema.length(), Utils.MAX_SCHEMA_LENGTH_IN_WARNING + 3);
    Assert.assertTrue(truncatedSchema.endsWith("..."));
    Assert.assertTrue(schema.toString().startsWith(truncatedSchema.substring(0, Utils.MAX_SCHEMA_LENGTH_IN_WARNING)));
  }
}
