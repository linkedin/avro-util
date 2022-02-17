/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avro.codegen.testutil.CompilerHelper;
import com.linkedin.avroutil1.model.AvroEnumSchema;
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
}
