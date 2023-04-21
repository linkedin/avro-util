/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaDifference;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ConfigurableAvroSchemaComparatorTest {

  @DataProvider
  private Object[][] testEqualsProvider() {
    return new Object[][]{{"schemas/TestRecord.avsc", "schemas/TestRecord.avsc", true},
        {"schemas/UtilTester1.avsc", "schemas/UtilTester2.avsc", false}};
  }

  @DataProvider
  private Object[][] testFindDifferenceProvider() {
    return new Object[][]{
        {"schemas/UtilTester1.avsc", "schemas/UtilTester1.avsc", SchemaComparisonConfiguration.PRE_1_7_3, 0},
        {"schemas/UtilTester1.avsc", null, SchemaComparisonConfiguration.PRE_1_7_3, 1},
        {"schemas/UtilTester1.avsc", "schemas/UtilTester2.avsc", SchemaComparisonConfiguration.PRE_1_7_3, 12},
        {"schemas/UtilTester1.avsc", "schemas/UtilTester2.avsc", SchemaComparisonConfiguration.STRICT, 17}};
  }

  private static AvroSchema validateAndGetAvroRecordSchema(String path) throws IOException {
    AvscParser parser = new AvscParser();
    String avsc = TestUtil.load(path);
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    return result.getTopLevelSchema();
  }

  @Test(dataProvider = "testEqualsProvider")
  public void testEquals(String path1, String path2, boolean expectedResult) throws IOException {
    AvroRecordSchema recordSchema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema(path1);
    AvroRecordSchema recordSchema2 = (AvroRecordSchema) validateAndGetAvroRecordSchema(path2);
    Assert.assertEquals(
        ConfigurableAvroSchemaComparator.equals(recordSchema1, recordSchema2, SchemaComparisonConfiguration.PRE_1_7_3),
        expectedResult);
  }

  @Test(dataProvider = "testFindDifferenceProvider")
  public void testFindDifference(String path1, String path2, SchemaComparisonConfiguration config,
      int expectedDifferences) throws IOException {
    // Load the schema, move this code to a separate method
    AvroRecordSchema schema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema(path1);
    AvroRecordSchema schema2 = (path2 != null) ? (AvroRecordSchema) validateAndGetAvroRecordSchema(path2) : null;
    List<AvroSchemaDifference> differences = ConfigurableAvroSchemaComparator.findDifference(schema1, schema2, config);
    Assert.assertEquals(differences.size(), expectedDifferences);
  }

  @Test
  public void testJsonProps() throws IOException {
    // test that json props are compared correctly in fields
    AvroRecordSchema schema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/TestJsonPropsInFields1.avsc");
    AvroRecordSchema schema2 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/TestJsonPropsInFields2.avsc");
    List<String> differences =
        ConfigurableAvroSchemaComparator.findDifference(schema1, schema2, SchemaComparisonConfiguration.STRICT)
            .stream()
            .map(diff -> diff.toString())
            .collect(Collectors.toList());
    Assert.assertEquals(differences.size(), 6);
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of record vs14.UtilTester1 in schemaA does not match with the json properties of record vs14.UtilTester1 in schemaB\n"
            + "SchemaALocation: lines 1-42. SchemaBLocation: lines 1-50"));
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of field \"fieldJsonPropMismatch\" in schemaA does not match with the json properties in schemaB\n"
            + "SchemaALocation: lines 6-9. SchemaBLocation: lines 6-10"));
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of float in schemaA does not match with the json properties of float in schemaB\n"
            + "SchemaALocation: line 12 columns 17-22. SchemaBLocation: lines 13-16"));
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of float[] in schemaA does not match with the json properties of float[] in schemaB\n"
            + "SchemaALocation: lines 16-19. SchemaBLocation: lines 20-24"));
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of enum vs14.EnumJsonPropMismatch in schemaA does not match with the json properties of enum vs14.EnumJsonPropMismatch in schemaB\n"
            + "SchemaALocation: lines 23-31. SchemaBLocation: lines 28-37"));
    Assert.assertTrue(differences.contains(
        "[JSON_PROPERTY_MISMATCH] Json properties of fixed vs14.FixedJsonPropMismatch in schemaA does not match with the json properties of fixed vs14.FixedJsonPropMismatch in schemaB\n"
            + "SchemaALocation: lines 35-39. SchemaBLocation: lines 41-46"));
  }
}
