/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.StringUtils;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.testutil.CompilerHelper;
import java.io.IOException;
import javax.tools.JavaFileObject;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SpecificRecordClassGeneratorTest {
  @DataProvider
  Object[][] schemaPaths() {
    return new Object[][]{
        {"schemas/ArrayOfRecords.avsc", false},
        {"schemas/ArrayOfStringRecord.avsc", true},
        {"schemas/BuilderTester.avsc", false},
        {"schemas/DollarSignInDoc.avsc", true},
        {"schemas/MoneyRange.avsc", false},
        {"schemas/RecordDefault.avsc", false},
        {"schemas/RecordWithRecordOfEnum.avsc", false},
        {"schemas/RecordWithRecordOfRecord.avsc", false},
        {"schemas/SimpleEnum.avsc", true},
        {"schemas/SimpleEnumWithHugeDoc.avsc", true},
        {"schemas/SimpleFixed.avsc", true},
        {"schemas/SimpleFixedWithHugeDoc.avsc", true},
        {"schemas/TestCollections.avsc", true},
        {"schemas/TestProblematicRecord.avsc", true},
        {"schemas/TestRecord.avsc", false}
    };
  }

  @Test(dataProvider = "schemaPaths")
  public void testSchema(String path, boolean checkCompiles) throws Exception {
    String avsc = TestUtil.load(path);
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroNamedSchema schema = (AvroNamedSchema) result.getTopLevelSchema();
    Assert.assertNotNull(schema);

    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    JavaFileObject javaFileObject =
        generator.generateSpecificClass(schema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);

    if (checkCompiles) {
      CompilerHelper.assertCompiles(javaFileObject);
    }
  }

  @Test
  public void testHasDeprecatedFieldsComment() throws IOException, ClassNotFoundException {
    String avsc = TestUtil.load("schemas/BuilderTester.avsc");
    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    AvscParser parser = new AvscParser();
    AvscParseResult result = parser.parse(avsc);
    Assert.assertNull(result.getParseError());
    AvroRecordSchema schema  = (AvroRecordSchema) result.getTopLevelSchema();

    JavaFileObject javaFileObject =
        generator.generateSpecificClass(schema, SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
    Assert.assertEquals(StringUtils.countMatches(javaFileObject.getCharContent(false).toString(),
            "/**\n" + "   * @deprecated public fields are deprecated. Please use setters/getters.\n" + "   */"),
        schema.getFields().size());
  }
}
