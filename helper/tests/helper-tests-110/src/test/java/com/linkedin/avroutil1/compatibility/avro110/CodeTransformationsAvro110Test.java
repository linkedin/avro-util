/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;

import under110wbuildersmin18.NormalRecordWithoutReferences;


public class CodeTransformationsAvro110Test {

  @Test
  public void testTransformAvro110Enum() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformEnumClass(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
    Assert.assertTrue(Enum.class.isAssignableFrom(transformedClass));
  }

  @Test
  public void testTransformAvro110parseCalls() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_9, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testTransformAvro110HugeRecord() throws Exception {
    String avsc = TestUtil.load("HugeRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_9, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testRemoveAvro110Builder() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.removeBuilderSupport(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testTransformAvro110Externalizable() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformExternalizableSupport(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testPacifyModel$Declaration() throws Exception {
    String avsc = TestUtil.load("RecordWithLogicalTypes.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.pacifyModel$Declaration(originalCode, AvroVersion.earliest(), AvroVersion.latest());
    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
    Assert.assertNotNull(transformedClass.getDeclaredField("MODEL$"));
  }

  @Test
  public void testKeepCustomCoders() throws Exception {
    String avsc = TestUtil.load("RecordWith144Fields.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);
    CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), originalCode); //vanilla compiles
    //min version = current should keep coders (if they are small enough)
    String transformedCode = CodeTransformations.applyAll(
            originalCode,
            AvroCompatibilityHelper.getRuntimeAvroVersion(),
            AvroCompatibilityHelper.getRuntimeAvroVersion(),
            AvroVersion.latest(),
            null
    );
    Class<?> fixedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(fixedClass); //compiles
    Method customDecodeMethod = fixedClass.getMethod("customDecode", ResolvingDecoder.class);
    Assert.assertNotNull(customDecodeMethod);
    Assert.assertEquals(customDecodeMethod.getDeclaringClass().getName(), schema.getFullName()); //actual impl
  }

  @Test
  public void testStripCustomCoders() throws Exception {
    String avsc = TestUtil.load("RecordWith432Fields.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);
    try {
      CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), originalCode);
      Assert.fail("compilation of vanilla-generated class expected to fail");
    } catch (ClassNotFoundException expected) {
      //sadly cant assert on root cause, but its "code too large" for RecordWith1000Fields.customDecode()
      //(over 64KBytes of bytecode)
    }

    //min version = current should keep coders (if they are small enough)
    String transformedCode = CodeTransformations.applyAll(
            originalCode,
            AvroCompatibilityHelper.getRuntimeAvroVersion(),
            AvroCompatibilityHelper.getRuntimeAvroVersion(),
            AvroVersion.latest(),
            null
    );

    Class<?> fixedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(fixedClass); //compiles
    Method customDecodeMethod = fixedClass.getMethod("customDecode", ResolvingDecoder.class);
    Assert.assertNotNull(customDecodeMethod);
    Assert.assertEquals(customDecodeMethod.getDeclaringClass().getName(), "org.apache.avro.specific.SpecificRecordBase"); //inherited default impl
  }

  @Test
  public void testBuilders() throws Exception {
    Assert.assertNotNull(NormalRecordWithoutReferences.newBuilder());
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
    Assert.assertTrue(enhancedCode.contains("public void setIntField(long value)"));
    Assert.assertTrue(enhancedCode.contains("public void setLongField(int value)"));
    Assert.assertTrue(enhancedCode.contains("public void setBoxedIntField(java.lang.Long value)"));
    Assert.assertTrue(enhancedCode.contains("public void setBoxedLongField(java.lang.Integer value)"));

    // Compile the enhanced code to verify it's valid Java
    Class<?> generatedClass = null;
    try {
      generatedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), enhancedCode);
    } catch (Exception e) {
      Assert.fail("Enhanced setter methods code should compile without errors");
    }

    Assert.assertNotNull(generatedClass.getMethod("setIntField", long.class));
    Assert.assertNotNull(generatedClass.getMethod("setLongField", int.class));
    Assert.assertNotNull(generatedClass.getMethod("setBoxedIntField", java.lang.Long.class));
    Assert.assertNotNull(generatedClass.getMethod("setBoxedLongField", java.lang.Integer.class));
  }

  @Test
  public void testAddOverloadedNumericConstructor() throws Exception {
    // Use the IntsAndLongs schema for testing
    String avsc = TestUtil.load("IntsAndLongs.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    // Apply the transformation
    String enhancedCode = CodeTransformations.addOverloadedNumericConstructor(originalCode);

    // Verify the enhanced code contains the overloaded constructor with swapped types
    Assert.assertTrue(enhancedCode.contains("public " + schema.getName() + "(java.lang.Integer longField, java.lang.Long "
        + "intField, java.lang.Integer boxedLongField, java.lang.Long boxedIntField)"));

    // Compile the enhanced code to verify it's valid Java
    Class<?> generatedClass = null;
    try {
      generatedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), enhancedCode);
    } catch (Exception e) {
      Assert.fail("Enhanced constructor code should compile without errors");
    }

    Assert.assertNotNull(generatedClass.getConstructor(
        java.lang.Long.class, java.lang.Integer.class, java.lang.Long.class, java.lang.Integer.class));
    Assert.assertNotNull(generatedClass.getConstructor(
        java.lang.Integer.class, java.lang.Long.class, java.lang.Integer.class, java.lang.Long.class));
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
