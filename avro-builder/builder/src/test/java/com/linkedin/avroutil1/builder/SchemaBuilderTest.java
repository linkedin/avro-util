/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.builder.operations.codegen.CodeGenerator;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SchemaBuilderTest {

  @Test
  public void testSimpleProjectUsingVanilla() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "simple-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath()
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 2);
  }

  @Test
  public void testSimpleProjectWithPlugin() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "simple-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--extraFileName", new File(outputFolder, "dummy.txt").getAbsolutePath()
    });
    //see output was generated
    List<Path> txtFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().equals("dummy.txt")
    ).collect(Collectors.toList());
    Assert.assertEquals(txtFiles.size(), 1);
  }

  @Test
  public void testSimpleProjectUsingOwnCodegen() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "simple-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name()
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 2);
  }

  @Test(expectedExceptions = java.lang.RuntimeException.class)
  public void testSimpleProjectWithFailOnDupsOption() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "dups-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name(),
        "--onDups", DuplicateSchemaBehaviour.FAIL.name()
    });
  }

  @Test
  public void testWithImportsFromClasspath() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "classpath-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name(),
        "--includeClasspath", Boolean.toString(true)
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 1);
  }

  @Test
  public void testWithImportsOnBothClasspathAndInFile() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "classpath-dup-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name(),
        "--includeClasspath", Boolean.toString(true)
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith("SchemaWithClasspathImport.java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 1);
    String fileContents = new String(Files.readAllBytes(javaFiles.get(0)), StandardCharsets.UTF_8);
    // This field name exists in the schema that's in the filesystem and does not exist in the schema on the classpath.
    Assert.assertTrue(fileContents.contains("lookAtMe"));
  }

  @Test
  public void testImportableSchemasUsingOwnCodegen() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "test-includes-option");
    File inputFolder = new File(simpleProjectRoot, "input");
    File includesFolder = new File(simpleProjectRoot, "common");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", includesFolder.getAbsolutePath(),
        "--non-importable-source", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name()
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 2);
  }

  @DataProvider
  Object[][] testUnderscoreProvider() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "testUnderscoreProvider")
  public void testUnderscores(boolean useOwnCodegen) throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "test-underscores");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(
        new String[]{"--input", inputFolder.getAbsolutePath(), "--output", outputFolder.getAbsolutePath(),
            "--generator", useOwnCodegen ? CodeGenerator.AVRO_UTIL.name() : CodeGenerator.VANILLA.name()});
    //see output was generated
    Optional<Path> javaFile = Files.find(outputFolder.toPath(), 5,
            (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java"))
        .collect(Collectors.toList())
        .stream()
        .findFirst();
    Assert.assertTrue(javaFile.isPresent());

    String file = new String(Files.readAllBytes(javaFile.get()));

    Assert.assertTrue(file.contains("getFieldWithUnderscores()"));
    Assert.assertTrue(file.contains("getFieldWithDoubleUnderscores()"));
  }

  @Test
  public void testTopLevelUnionUsingOwnCodegen() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "union-schema-project");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name(),
        "enableUtf8EncodingInPutByIndex", "false"
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 2);
  }

  private File locateTestProjectsRoot() {
    //the current working directory for test execution varies across gradle and IDEs.
    //as such, we need to get creative to locate the folder
    URL url = Thread.currentThread().getContextClassLoader().getResource("test-projects");
    if (url == null) {
      throw new IllegalStateException("unable to find \"test-projects\" folder");
    }
    if (!url.getProtocol().equals("file")) {
      throw new IllegalStateException(url + " is a " + url.getProtocol() + " and not a file/folder");
    }
    File file = new File(url.getPath()); //getPath() should be an absolute path string
    if (!file.exists()) {
      throw new IllegalStateException("test-projects root folder " + file.getAbsolutePath() + " does not exist");
    }
    if (!file.isDirectory() || !file.canRead()) {
      throw new IllegalStateException("test-projects root folder " + file.getAbsolutePath() + " not a folder or is unreadable");
    }
    return file;
  }

  @Test
  public void testAvroCompatibilitys() throws IOException {
    Schema writerSchema = AvroCompatibilityHelper.parse(load("latest-in-SR.avsc"));
    Schema readerSchemaOriginal = AvroCompatibilityHelper.parse(load("new-schema.avsc"));

    String mitigatedString =
        AvroCompatibilityHelper.toAvsc(readerSchemaOriginal, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);
    Schema readerSchemaFinal = AvroCompatibilityHelper.parse(mitigatedString);

    GenericRecord datum =
        (GenericRecord) new RandomRecordGenerator().randomGeneric(writerSchema, RecordGenerationConfig.NO_NULLS);

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);

    // Write generic record out  with old schema
    byte[] avroBytes;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(outputStream, false, null);
      writer.write(datum, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }

    // Attempt to read into new reader schema
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(avroBytes);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, readerSchemaFinal);

    IndexedRecord deserialized = reader.read(null, decoder);
    Assert.assertNotNull(deserialized);
  }

  public static String load(String path) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    if (is == null) {
      throw new IllegalArgumentException("resource " + path + " not found on context classloader");
    }
    try {
      return IOUtils.toString(is, "utf-8");
    } finally {
      is.close();
    }
  }

}
