package com.linkedin.avro.fastserde;

import java.util.Arrays;

import javax.lang.model.SourceVersion;

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
}
