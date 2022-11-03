/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.testutil.CompilerHelper;
import javax.tools.JavaFileObject;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SpecificRecordClassGeneratorTest {

  @Test
  public void testSimpleEnum() throws Exception {
    String avsc = TestUtil.load("schemas/SimpleEnum.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroEnumSchema enumSchema = (AvroEnumSchema) result.getTopLevelSchema();
    Assert.assertNotNull(enumSchema);
    JavaFileObject javaFileObject = generator.generateSpecificClass(enumSchema,
        SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    CompilerHelper.assertCompiles(javaFileObject);

  }

  @Test
  public void testHugeEnum() throws Exception {
    String avsc = TestUtil.load("schemas/SimpleEnumWithHugeDoc.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroEnumSchema enumSchema = (AvroEnumSchema) result.getTopLevelSchema();
    Assert.assertNotNull(enumSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(enumSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    CompilerHelper.assertCompiles(javaFileObject);

  }

  @Test
  public void testSimpleFixed() throws Exception {
    String avsc = TestUtil.load("schemas/SimpleFixed.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroFixedSchema fixedSchema = (AvroFixedSchema) result.getTopLevelSchema();
    Assert.assertNotNull(fixedSchema);
    JavaFileObject javaFileObject = generator.generateSpecificClass(fixedSchema,
        SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    CompilerHelper.assertCompiles(javaFileObject);

  }

  @Test
  public void testSimpleFixedWithHugeDoc() throws Exception {
    String avsc = TestUtil.load("schemas/SimpleFixedWithHugeDoc.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroFixedSchema fixedSchema = (AvroFixedSchema) result.getTopLevelSchema();
    Assert.assertNotNull(fixedSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(fixedSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    CompilerHelper.assertCompiles(javaFileObject);

  }

  @DataProvider
  Object[][] testRecordWithArrayOfRecordsProvider() {
    return new Object[][]{
        {"schemas/ArrayOfStringRecord.avsc"},
        {"schemas/ArrayOfRecords.avsc"},
        {"schemas/TestCollections.avsc"}
    };
  }

  @Test(dataProvider = "testRecordWithArrayOfRecordsProvider")
  public void testRecordWithArrayOfRecords(String path) throws Exception {
    String avsc = TestUtil.load(path);
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

  }

  @DataProvider
  Object[][] testSpecificWithInternalClassesProvider() {
    return new Object[][]{
        {"schemas/RecordWithRecordOfEnum.avsc"},
        {"schemas/RecordWithRecordOfRecord.avsc"},
        {"schemas/TestRecord.avsc"}
    };
  }

  @Test(dataProvider = "testSpecificWithInternalClassesProvider")
  public void testSpecificWithInternalClasses(String filePath) throws Exception {
    String avsc = TestUtil.load(filePath);
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaSourceFile = generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
  }

  @Test
  public void testSpecificWith$InDoc() throws Exception {
    String avsc = TestUtil.load("schemas/DollarSignInDoc.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    CompilerHelper.assertCompiles(javaFileObject);
  }

  @Test
  public void testSpecificRecordWithInternalDefinedTypeReuse() throws Exception {
    String avsc = TestUtil.load("schemas/MoneyRange.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
  }

  @Test
  public void testDefaultForRecord() throws Exception {
    String avsc = TestUtil.load("schemas/RecordDefault.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
  }

  @Test
  public void testRecord() throws Exception {
    String avsc = TestUtil.load("schemas/BuilderTester.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
  }

}
