/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.builder.operations.codegen.CodeGenerator;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
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
  public void testWithImportsFromClasspath_skipLocallyDefined() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "classpath-project-with-skip");
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
        "--includeClasspath", Boolean.toString(true),
        "--skipCodegenIfSchemaOnClasspath", Boolean.toString(true)
    });
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 1);
  }

  @Test
  public void testWhenSameFQCNNotEqualOnClasspathAndInline() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "test-classpath-and-inline-schemas-are-not-equal");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    Assert.assertThrows(java.lang.IllegalStateException.class,
      () -> SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath(),
        "--generator", CodeGenerator.AVRO_UTIL.name(),
        "--includeClasspath", Boolean.toString(true),
        "--skipCodegenIfSchemaOnClasspath", Boolean.toString(true)
    }));
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
}
