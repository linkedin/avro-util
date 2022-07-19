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
    SchemaBuilder.main(new String[] {"--input", inputFolder.getAbsolutePath(), "--output", outputFolder.getAbsolutePath()});
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 1);
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

  //TODO - complete this
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
    Assert.assertEquals(javaFiles.size(), 1);
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
