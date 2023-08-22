package com.linkedin.avro.fastserde;

import java.util.Arrays;

import javax.lang.model.SourceVersion;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class UtilsTest {

  @Test (groups = "deserializationTest")
  public void testGenerateSourcePathFromPackageName() {
    Assert.assertEquals(Utils.generateSourcePathFromPackageName("com.linkedin.avro"), Utils.fixSeparatorsToMatchOS("/com/linkedin/avro/"));
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
  void shouldGenerateValidJavaIdentifier(String varNameToPolish) {
    // when
    String validJavaIdentifier = Utils.toValidJavaIdentifier(varNameToPolish);

    // when
    Assert.assertTrue(SourceVersion.isIdentifier(validJavaIdentifier));
  }

  /*-----------------------------------------*/

  @DataProvider
  static Object[][] createInvalidJavaIdentifierTestCases() {
    String[] validOrAlmostValidJavaIdentifiers = {
            null,
            "",
            " ",
            "\n\t\r",
            "\n   \t   \r "
    };

    return Arrays.stream(validOrAlmostValidJavaIdentifiers)
            .map(str -> new Object[]{str})
            .toArray(Object[][]::new);
  }

  @Test(dataProvider = "createInvalidJavaIdentifierTestCases", expectedExceptions = NullPointerException.class)
  void shouldFailGeneratingValidJavaIdentifier(String invalidProposal) {
    // NPE expected
    Utils.toValidJavaIdentifier(invalidProposal);
  }
}
