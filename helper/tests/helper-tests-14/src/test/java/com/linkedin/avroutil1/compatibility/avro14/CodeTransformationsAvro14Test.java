/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;

import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificCompiler;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationsAvro14Test {

  @Test
  public void testTransformAvro14Enum() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformEnumClass(originalCode, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
    Assert.assertTrue(Enum.class.isAssignableFrom(transformedClass));
  }

  @Test
  public void testTransformAvro14HugeRecord() throws Exception {
    String avsc = TestUtil.load("HugeRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_4, AvroVersion.earliest(), AvroVersion.latest());

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);
  }

  @Test
  public void testTransformAvro14RecordWithMultilineDoc() throws Exception {
    String avsc = TestUtil.load("RecordWithMultilineDoc.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_4, AvroVersion.earliest(), AvroVersion.latest());

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
  public void testAlternateAvscUnderAvro14() throws Exception {
    String altAvsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    String avsc = TestUtil.load("HugeRecord.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_4, AvroVersion.earliest(), AvroVersion.latest(), altAvsc);

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);

    Schema inCode = (Schema) transformedClass.getField("SCHEMA$").get(null);
    Assert.assertEquals(inCode.getType(), Schema.Type.ENUM); //transplant successful
  }

  @Test
  public void testFixAvro702() throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    @SuppressWarnings("UnnecessaryLocalVariable")
    String altAvsc = avsc; //swap in the "good" (original) avsc
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    Assert.assertTrue(AvroSchemaUtil.isImpactedByAvro702(schema)); //make sure its still bad
    File outputRoot = Files.createTempDirectory(null).toFile();

    //the schema in question is complicated and generates 3 classes
    runNativeCodegen(schema, outputRoot);

    //get source code for 3 classes
    String innerCode = readJavaClass(outputRoot, "com.acme.outer.InnerRecord");
    String middleCode = readJavaClass(outputRoot, "com.acme.middle.MiddleRecord");
    String outerCode = readJavaClass(outputRoot, "com.acme.outer.OuterRecord");

    URL root = outputRoot.toURI().toURL();
    ClassLoader cl = new URLClassLoader(new URL[] {root}, Thread.currentThread().getContextClassLoader());

    //load 1st 2 classes as-is
    CompilerUtils.CACHED_COMPILER.loadFromJava(cl, "com.acme.outer.InnerRecord", innerCode);
    CompilerUtils.CACHED_COMPILER.loadFromJava(cl, "com.acme.middle.MiddleRecord", middleCode);

    //fix up SCHEMA$ in outer class
    String transformedCode = CodeTransformations.transformParseCalls(outerCode, AvroVersion.AVRO_1_4, AvroVersion.earliest(), AvroVersion.latest(), altAvsc);

    //load outer class
    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(cl, schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);

    //assert SCHEMA$ on outer class is "ok"
    Schema inCode = (Schema) transformedClass.getField("SCHEMA$").get(null);
    Assert.assertEquals(inCode, schema);
  }

  @Test
  public void testAlternateAvscUnderAvro14WithEscaping() throws Exception {
    String altAvsc = TestUtil.load("RecordWithMultilineDoc.avsc");
    String avsc = TestUtil.load("RecordWithMultilineDoc.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema);

    String transformedCode = CodeTransformations.transformParseCalls(originalCode, AvroVersion.AVRO_1_4, AvroVersion.earliest(), AvroVersion.latest(), altAvsc);

    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), transformedCode);
    Assert.assertNotNull(transformedClass);

    Schema inCode = (Schema) transformedClass.getField("SCHEMA$").get(null);

    Assert.assertEquals(inCode, schema); //no (significant) harm done
  }

  private String runNativeCodegen(Schema schema) throws Exception {
    File outputRoot = Files.createTempDirectory(null).toFile();
    return runNativeCodegen(schema, outputRoot);
  }

  private String runNativeCodegen(Schema schema, File outputRoot) throws Exception {
    Method compilerCompileToDestinationMethod = SpecificCompiler.class.getDeclaredMethod("compileToDestination", File.class, File.class);
    compilerCompileToDestinationMethod.setAccessible(true); //private

    SpecificCompiler compiler = new SpecificCompiler(schema);
    compilerCompileToDestinationMethod.invoke(compiler, null, outputRoot);

    return readJavaClass(outputRoot, schema.getFullName());
  }

  private String readJavaClass(File outputRoot, String fqcn) throws Exception {
    File javaFile = new File(outputRoot, fqcn.replaceAll("\\.", Matcher.quoteReplacement(File.separator)) + ".java");
    Assert.assertTrue(javaFile.exists());

    String sourceCode;
    try (FileInputStream fis = new FileInputStream(javaFile)) {
      sourceCode = IOUtils.toString(fis, StandardCharsets.UTF_8);
    }
    return sourceCode;
  }
}
