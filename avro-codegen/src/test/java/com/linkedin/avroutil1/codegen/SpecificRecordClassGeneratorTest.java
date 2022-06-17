/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avro.codegen.testutil.CompilerHelper;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import javax.tools.JavaFileObject;
import org.testng.Assert;
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
    JavaFileObject javaSourceFile = generator.generateSpecificRecordClass(enumSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    CompilerHelper.assertCompiles(javaSourceFile);
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
    JavaFileObject javaSourceFile = generator.generateSpecificRecordClass(enumSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    CompilerHelper.assertCompiles(javaSourceFile);
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
    JavaFileObject javaSourceFile = generator.generateSpecificRecordClass(fixedSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    CompilerHelper.assertCompiles(javaSourceFile);
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
    JavaFileObject javaSourceFile = generator.generateSpecificRecordClass(fixedSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    CompilerHelper.assertCompiles(javaSourceFile);
  }

  @Test
  public void testSpecific() throws Exception {
    String avsc = TestUtil.load("schemas/TestRecord.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema recordSchema = (AvroRecordSchema) result.getTopLevelSchema();
    Assert.assertNotNull(recordSchema);
    JavaFileObject javaSourceFile = generator.generateSpecificRecordClass(recordSchema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    CompilerHelper.assertCompiles(javaSourceFile);
  }

}
