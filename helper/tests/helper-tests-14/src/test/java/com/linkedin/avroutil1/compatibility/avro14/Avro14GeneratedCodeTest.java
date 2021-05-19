/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificCompiler;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;


public class Avro14GeneratedCodeTest {

  @Test(expectedExceptions = ExceptionInInitializerError.class)
  public void demonstrateBadCodeForMultilineDocs() {
    new by14.RecordWithMultilineDoc();
  }

  @Test(expectedExceptions = ClassNotFoundException.class)
  public void demonstrateRecordWithComOrgFields() throws Exception {
    String avsc = TestUtil.load("RecordWithComOrgFields.avsc");
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    String originalCode = runNativeCodegen(schema, avsc);

    CompilerUtils.CACHED_COMPILER.loadFromJava(schema.getFullName(), originalCode);
    Assert.fail("not expected to compile");
  }

  private String runNativeCodegen(Schema schema, String avsc) throws Exception {
    File outputRoot = Files.createTempDirectory(null).toFile();
    File schemaFile = Files.createTempFile("schema", "avsc").toFile();
    try (FileOutputStream fos = new FileOutputStream(schemaFile)) {
      IOUtils.write(avsc, fos, StandardCharsets.UTF_8);
    }
    SpecificCompiler.compileSchema(schemaFile, outputRoot);
    File javaFile = new File(outputRoot, schema.getNamespace().replaceAll("\\.", Matcher.quoteReplacement(File.separator)) + File.separator + schema.getName() + ".java");
    Assert.assertTrue(javaFile.exists());

    String sourceCode;
    try (FileInputStream fis = new FileInputStream(javaFile)) {
      sourceCode = IOUtils.toString(fis, StandardCharsets.UTF_8);
    }

    return sourceCode;
  }
}
