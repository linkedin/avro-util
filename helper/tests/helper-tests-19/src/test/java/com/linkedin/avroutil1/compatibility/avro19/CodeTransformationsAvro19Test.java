/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;

import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationsAvro19Test {

  @Test
  public void testTransformAvro19Enum() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformEnumClass(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
    Assert.assertTrue(Enum.class.isAssignableFrom(transformedClass));
  }

  @Test
  public void testTransformAvro19parseCalls() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_9, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testTransformAvro19HugeRecord() throws Exception {
    String avsc = TestUtil.load("HugeRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_9, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testRemoveAvro19Builder() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.removeBuilderSupport(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testTransformAvro19Externalizable() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformExternalizableSupport(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
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
