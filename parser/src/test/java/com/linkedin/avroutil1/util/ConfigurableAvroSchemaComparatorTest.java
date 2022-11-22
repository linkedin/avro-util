/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ConfigurableAvroSchemaComparatorTest {

  @DataProvider
  private Object[][] testEqualsProvider() {
    return new Object[][] {
        {"schemas/TestRecord.avsc", "schemas/TestRecord.avsc", true}
    };
  }

  @Test(dataProvider = "testEqualsProvider")
  public void testEquals(String path1, String path2, boolean isEqual) throws IOException {
    AvscParser parser = new AvscParser();
    String avsc1 = TestUtil.load(path1);
    String avsc2 = TestUtil.load(path2);
    AvscParseResult result1 = parser.parse(avsc1);
    AvscParseResult result2 = parser.parse(avsc2);
    Assert.assertNull(result1.getParseError());
    Assert.assertNull(result2.getParseError());
    AvroRecordSchema recordSchema1 = (AvroRecordSchema) result1.getTopLevelSchema();
    AvroRecordSchema recordSchema2 = (AvroRecordSchema) result2.getTopLevelSchema();
    Assert.assertTrue(
        ConfigurableAvroSchemaComparator.equals(recordSchema1, recordSchema2, SchemaComparisonConfiguration.PRE_1_7_3));
  }

}
