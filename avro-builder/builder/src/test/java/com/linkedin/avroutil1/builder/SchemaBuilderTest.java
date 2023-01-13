/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.builder.operations.codegen.CodeGenerator;
import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
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
}
