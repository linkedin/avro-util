/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationsAvro16Test {

  @Test
  public void testTransformAvro16Enum() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformEnumClass(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
    Assert.assertTrue(Enum.class.isAssignableFrom(transformedClass));
  }

  @Test
  public void testTransformAvro16HugeRecord() throws Exception {
    String avsc = TestUtil.load("HugeRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_6, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testEnhanceNumericPutMethod() throws Exception {
    // Use the IntsAndLongs schema for testing
    String avsc = TestUtil.load("IntsAndLongs.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    // Apply the transformation
    String enhancedCode = CodeTransformations.enhanceNumericPutMethod(originalCode);

    // Verify the enhanced code contains the type checking for numeric conversions
    Assert.assertTrue(enhancedCode.contains("if (value$ instanceof java.lang.Long)"));
    Assert.assertTrue(enhancedCode.contains("if (value$ instanceof java.lang.Integer)"));

    // Compile the enhanced code to verify it's valid Java
    try {
      CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), enhancedCode);
    } catch (Exception e) {
      Assert.fail("Enhanced put method code should compile without errors");
    }
  }

  @Test
  public void testAddOverloadedNumericSetterMethods() throws Exception {
    // Use the IntsAndLongs schema for testing
    String avsc = TestUtil.load("IntsAndLongs.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    // Apply the transformation
    String enhancedCode = CodeTransformations.addOverloadedNumericSetterMethods(originalCode);

    // Verify the enhanced code contains the overloaded setters
    Assert.assertTrue(enhancedCode.contains("public void setIntField(java.lang.Long value)"));
    Assert.assertTrue(enhancedCode.contains("public void setLongField(java.lang.Integer value)"));
    Assert.assertTrue(enhancedCode.contains("public void setBoxedIntField(java.lang.Long value)"));
    Assert.assertTrue(enhancedCode.contains("public void setBoxedLongField(java.lang.Integer value)"));

    // Compile the enhanced code to verify it's valid Java
    try {
      CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), enhancedCode);
    } catch (Exception e) {
      Assert.fail("Enhanced setter methods code should compile without errors");
    }
  }

  private String runNativeCodegen(Schema schema) throws Exception {
    File outputRoot = Files.createTempDirectory(null).toFile();
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.compileToDestination(null, outputRoot);
    File javaFile = new File(outputRoot, schema.getNamespace().replaceAll("\\.", Matcher.quoteReplacement(File.separator)) + File.separator + schema.getName() + ".java");
    Assert.assertTrue(javaFile.exists());

    String sourceCode;
    try (FileInputStream fis = new FileInputStream(javaFile)) {
      sourceCode = IOUtils.toString(fis, StandardCharsets.UTF_8);
    }

    return sourceCode;
  }
}
