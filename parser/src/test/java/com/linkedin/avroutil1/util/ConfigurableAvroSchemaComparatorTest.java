/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaDifference;
import com.linkedin.avroutil1.model.AvroSchemaDifferenceType;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ConfigurableAvroSchemaComparatorTest {

  @DataProvider
  private Object[][] testEqualsProvider() {
    return new Object[][] {
        {"schemas/TestRecord.avsc", "schemas/TestRecord.avsc"}
    };
  }

  private static AvroSchema validateAndGetAvroRecordSchema(String path) throws IOException {
    AvscParser parser = new AvscParser();
    String avsc = TestUtil.load(path);
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    return result.getTopLevelSchema();
  }

  @Test(dataProvider = "testEqualsProvider")
  public void testEquals(String path1, String path2) throws IOException {
    AvroRecordSchema recordSchema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema(path1);
    AvroRecordSchema recordSchema2 = (AvroRecordSchema) validateAndGetAvroRecordSchema(path2);
    Assert.assertTrue(ConfigurableAvroSchemaComparator.equals(recordSchema1, recordSchema2, SchemaComparisonConfiguration.PRE_1_7_3));
  }

  @Test
  public void testNotEquals() throws IOException {
    AvroRecordSchema schema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester1.avsc");
    AvroRecordSchema schema2 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester2.avsc");
    Assert.assertFalse(ConfigurableAvroSchemaComparator.equals(schema1, schema2, SchemaComparisonConfiguration.PRE_1_7_3));
  }

  // FindDifference testing
  @Test
  public void testSimilarSchema() throws IOException {
    // Load the schema, move this code to a separate method
    AvroRecordSchema schema = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester1.avsc");
    List<AvroSchemaDifference> differences = ConfigurableAvroSchemaComparator.findDifference(schema, schema, SchemaComparisonConfiguration.PRE_1_7_3);
    Assert.assertEquals(differences.size(), 0);
  }

  @Test
  public void testNullSchema() throws IOException {
    // Load the schema, move this code to a separate method
    AvroRecordSchema schema = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester1.avsc");
    List<AvroSchemaDifference> differences = ConfigurableAvroSchemaComparator.findDifference(schema, null, SchemaComparisonConfiguration.PRE_1_7_3);
    Assert.assertEquals(differences.size(), 1);
  }

  @Test
  public void testSchemaWithTypeMismatch() throws IOException  {
    AvroRecordSchema schema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester1.avsc");
    AvroEnumSchema schema2 = (AvroEnumSchema) validateAndGetAvroRecordSchema("schemas/TestEnum.avsc");
    List<AvroSchemaDifference> differences =  ConfigurableAvroSchemaComparator.findDifference(schema1, schema2, SchemaComparisonConfiguration.PRE_1_7_3);
    Assert.assertEquals(differences.size(), 1);
  }

  @Test
  public void testSchemaWithDifferences() throws IOException {

    AvroRecordSchema schema1 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester1.avsc");
    AvroRecordSchema schema2 = (AvroRecordSchema) validateAndGetAvroRecordSchema("schemas/UtilTester2.avsc");
    List<AvroSchemaDifference> differences = ConfigurableAvroSchemaComparator.findDifference(schema1, schema2, SchemaComparisonConfiguration.STRICT);
    int expectedDifferences = 18;
    AvroSchemaDifferenceType[] expectedDifferenceTypes = {
        AvroSchemaDifferenceType.RECORD_NAME_MISMATCH,
        AvroSchemaDifferenceType.ALIASES_MISMATCH,
        AvroSchemaDifferenceType.RECORD_FIELD_COUNT_MISMATCH,
        AvroSchemaDifferenceType.TYPE_MISMATCH,
        AvroSchemaDifferenceType.RECORD_DEFAULT_VALUE_MISMATCH,
        AvroSchemaDifferenceType.ALIASES_MISMATCH,
        AvroSchemaDifferenceType.ALIASES_MISMATCH,
        AvroSchemaDifferenceType.ENUM_SYMBOL_MISMATCH,
        AvroSchemaDifferenceType.ALIASES_MISMATCH,
        AvroSchemaDifferenceType.FIXED_SIZE_MISMATCH,
        AvroSchemaDifferenceType.TYPE_MISMATCH,
        AvroSchemaDifferenceType.TYPE_MISMATCH,
        AvroSchemaDifferenceType.TYPE_MISMATCH,
        AvroSchemaDifferenceType.UNION_SIZE_MISMATCH,
        AvroSchemaDifferenceType.ALIASES_MISMATCH,
        AvroSchemaDifferenceType.FIXED_SIZE_MISMATCH,
        AvroSchemaDifferenceType.RECORD_FIELD_NAME_MISMATCH,
        AvroSchemaDifferenceType.ADDITIONAL_FIELD
    };

    Assert.assertEquals(differences.size(), expectedDifferences);
    for (int i = 0; i < expectedDifferences; i++) {
      Assert.assertEquals(differences.get(i).getAvroSchemaDifferenceType(), expectedDifferenceTypes[i]);
    }
  }

}
