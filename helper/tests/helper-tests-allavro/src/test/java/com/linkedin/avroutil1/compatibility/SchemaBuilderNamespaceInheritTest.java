/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SchemaBuilderNamespaceInheritTest {

  @DataProvider
  private Object[][] TestSchemeBuildWithNamespaceInheritProvider() {
    return new Object[][]{
        {"allavro/ComplexRecord.avsc", "allavro/ComplexRecordInherit.avsc", true},
        {"allavro/ComplexRecordInnerNamespace.avsc", "allavro/ComplexRecordInnerNamespaceInherit.avsc", true},
        {"allavro/ComplexRecord.avsc", "allavro/ComplexRecordNoInherit.avsc", false},
        {"allavro/ComplexRecordInnerNamespace.avsc", "allavro/ComplexRecordInnerNamespaceNoInherit.avsc", false}
    };
  }

  @Test(dataProvider = "TestSchemeBuildWithNamespaceInheritProvider")
  public void testSchemaBuilderWithNamespaceInherit(String originalAvscFile, String resultAvscFile,
      boolean inheritNamespace) throws Exception {
    String nameSpace = "TopNamespace";

    String originalAvsc = TestUtil.load(originalAvscFile);
    Schema originalSchema = Schema.parse(originalAvsc);

    //build new schema and add namespace at top level
    Schema newSchema = AvroCompatibilityHelper.newSchema(originalSchema)
        .setNamespace(nameSpace)
        .setInheritNamespace(inheritNamespace)
        .build();

    validateSchema(originalSchema, newSchema, nameSpace, inheritNamespace, 0);

    String newAvsc = AvroCompatibilityHelper.toAvsc(newSchema, AvscGenerationConfig.CORRECT_PRETTY);
    //on windows newAvsc has \r\n for linebreaks, which fails the check below
    String newAvscSansCarriageReturns = newAvsc.replaceAll("\r", "");
    String targetAvsc = TestUtil.load(resultAvscFile);
    Assert.assertEquals(newAvscSansCarriageReturns, targetAvsc);
  }

  private void validateSchema(Schema originalSchema, Schema newSchema, String parentNameSpace, boolean inheritNamespace,
      int level) {
    if (originalSchema == null && newSchema == null) {
      return;
    }

    Assert.assertNotNull(originalSchema);
    Assert.assertNotNull(newSchema);

    Assert.assertEquals(newSchema.getType(), originalSchema.getType());

    switch (originalSchema.getType()) {
      case FIXED:
        Assert.assertEquals(newSchema.getName(), originalSchema.getName());
        Assert.assertEquals(newSchema.getDoc(), originalSchema.getDoc());
        String namespace = inheritNamespace ? Optional.ofNullable(originalSchema.getNamespace()).orElse(parentNameSpace)
            : originalSchema.getNamespace();
        Assert.assertEquals(newSchema.getNamespace(), namespace);
        Assert.assertEquals(newSchema.getFixedSize(), originalSchema.getFixedSize());
        break;
      case ENUM:
        Assert.assertEquals(newSchema.getName(), originalSchema.getName());
        Assert.assertEquals(newSchema.getDoc(), originalSchema.getDoc());
        namespace = inheritNamespace ? Optional.ofNullable(originalSchema.getNamespace()).orElse(parentNameSpace)
            : originalSchema.getNamespace();
        Assert.assertEquals(newSchema.getNamespace(), namespace);
        Assert.assertEquals(newSchema.getEnumSymbols(), originalSchema.getEnumSymbols());
        break;
      case RECORD:
        Assert.assertEquals(newSchema.getName(), originalSchema.getName());
        Assert.assertEquals(newSchema.getDoc(), originalSchema.getDoc());
        namespace =
            inheritNamespace || level == 0 ? Optional.ofNullable(originalSchema.getNamespace()).orElse(parentNameSpace)
                : originalSchema.getNamespace();
        Assert.assertEquals(newSchema.getNamespace(), namespace);
        Assert.assertEquals(newSchema.isError(), originalSchema.isError());

        validateFields(originalSchema.getFields(), newSchema.getFields(), parentNameSpace, inheritNamespace, level + 1);

        break;
      case ARRAY:
        validateSchema(originalSchema.getElementType(), newSchema.getElementType(), parentNameSpace, inheritNamespace,
            level + 1);
        break;
      case MAP:
        validateSchema(originalSchema.getValueType(), newSchema.getValueType(), parentNameSpace, inheritNamespace,
            level + 1);
        break;
      case UNION:
        validateSchemas(originalSchema.getTypes(), newSchema.getTypes(), parentNameSpace, inheritNamespace, level + 1);
        break;
    }
  }

  private void validateFields(List<Schema.Field> originalFields, List<Schema.Field> newFields, String parentNamespace,
      boolean inheritNamespace, int level) {
    if (originalFields == null && newFields == null) {
      return;
    }

    Assert.assertNotNull(originalFields);
    Assert.assertNotNull(newFields);

    Assert.assertEquals(originalFields.size(), newFields.size());

    for (int i = 0; i < originalFields.size(); i++) {
      validateSchema(originalFields.get(i).schema(), newFields.get(i).schema(), parentNamespace, inheritNamespace,
          level);
    }
  }

  private void validateSchemas(List<Schema> originalSchemas, List<Schema> newSchemas, String parentNamespace,
      boolean inheritNamespace, int level) {
    if (originalSchemas == null && newSchemas == null) {
      return;
    }

    Assert.assertNotNull(originalSchemas);
    Assert.assertNotNull(newSchemas);

    Assert.assertEquals(originalSchemas.size(), newSchemas.size());

    for (int i = 0; i < originalSchemas.size(); i++) {
      validateSchema(originalSchemas.get(i), newSchemas.get(i), parentNamespace, inheritNamespace, level);
    }
  }
}