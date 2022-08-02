/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen;

import com.linkedin.avroutil1.builder.AvroSchemaBuilderUtils;
import com.linkedin.avroutil1.builder.DuplicateSchemaBehaviour;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * holds the parsed and validated configuration for a code-generation operation
 */
public class CodeGenOpConfig {

  //inputs

  List<File> inputRoots;
  List<File> nonImportableSourceRoots;
  boolean includeClasspath;

  //outputs

  File outputSpecificRecordClassesRoot;
  File outputExpandedSchemasRoot;

  //other knobs

  CodeGenerator generatorType;
  DuplicateSchemaBehaviour dupBehaviour;
  List<String> duplicateSchemasToIgnore; //by fullname

  StringRepresentation stringRepresentation;
  AvroVersion minAvroVersion;
  boolean avro702Handling;

  public CodeGenOpConfig(
      List<File> inputRoots,
      List<File> nonImportableSourceRoots,
      boolean includeClasspath,
      File outputSpecificRecordClassesRoot,
      File outputExpandedSchemasRoot,
      CodeGenerator generatorType,
      DuplicateSchemaBehaviour dupBehaviour,
      List<String> duplicateSchemasToIgnore,
      StringRepresentation stringRepresentation,
      AvroVersion minAvroVersion,
      boolean avro702Handling
  ) {
    this.inputRoots = inputRoots;
    this.nonImportableSourceRoots = nonImportableSourceRoots;
    this.includeClasspath = includeClasspath;
    this.outputSpecificRecordClassesRoot = outputSpecificRecordClassesRoot;
    this.outputExpandedSchemasRoot = outputExpandedSchemasRoot;
    this.generatorType = generatorType;
    this.dupBehaviour = dupBehaviour;
    this.duplicateSchemasToIgnore = duplicateSchemasToIgnore;
    this.stringRepresentation = stringRepresentation;
    this.minAvroVersion = minAvroVersion;
    this.avro702Handling = avro702Handling;
  }

  /**
   * validates all input parameters (set at construction time)
   */
  public void validateParameters() {

    if (inputRoots == null || inputRoots.isEmpty()) {
      throw new IllegalArgumentException("must provide at least one input path");
    }
    validateInput(inputRoots, "input");
    List<File> inputsAndIncludes = new ArrayList<>(inputRoots);

    if (nonImportableSourceRoots != null) {
      if (nonImportableSourceRoots.isEmpty()) {
        nonImportableSourceRoots = null;
      } else {
        validateInput(nonImportableSourceRoots, "non-importable-source");
        inputsAndIncludes.addAll(nonImportableSourceRoots);
      }
    }

    //input/includes should not overlap - meaning neither should be a parent folder of the other
    //(otherwise schemas will be picked up twice and the run will fail for duplicates)
    //the following is brutish, but how many input folders do we really expect ? :-)
    for (File anyFile : inputsAndIncludes) {
      for (File anyOtherFile : inputsAndIncludes) {
        if (anyFile == anyOtherFile) {
          continue;
        }
        if (AvroSchemaBuilderUtils.isParentOf(anyOtherFile, anyFile)) { //other direction will be covered later in the loop
          throw new IllegalArgumentException("input/include paths " + anyFile + " and " + anyOtherFile + " overlap");
        }
      }
    }

    if (outputSpecificRecordClassesRoot == null) {
      throw new IllegalArgumentException("must provide output root for specific class generation");
    }
    validateOutput(outputExpandedSchemasRoot, "specific classes output");
    //TODO - mkdirs on execution
    validateOutput(outputExpandedSchemasRoot, "expanded schemas output");

    if (generatorType == null) {
      throw new IllegalArgumentException("generator type must be specified");
    }
    if (generatorType == CodeGenerator.VANILLA) {
      //see the avro compiler exists
      if (AvroCompatibilityHelper.getRuntimeAvroCompilerVersion() == null) {
        throw new IllegalStateException("unable to find org.apache.avro:avro-compiler on the classpath");
      }
    }
  }

  public List<File> getInputRoots() {
    return inputRoots;
  }

  public List<File> getNonImportableSourceRoots() {
    return nonImportableSourceRoots;
  }

  public boolean isIncludeClasspath() {
    return includeClasspath;
  }

  public File getOutputSpecificRecordClassesRoot() {
    return outputSpecificRecordClassesRoot;
  }

  public File getOutputExpandedSchemasRoot() {
    return outputExpandedSchemasRoot;
  }

  public CodeGenerator getGeneratorType() {
    return generatorType;
  }

  public DuplicateSchemaBehaviour getDupBehaviour() {
    return dupBehaviour;
  }

  public List<String> getDuplicateSchemasToIgnore() {
    return duplicateSchemasToIgnore;
  }

  public StringRepresentation getStringRepresentation() {
    return stringRepresentation;
  }

  public AvroVersion getMinAvroVersion() {
    return minAvroVersion;
  }

  public boolean isAvro702Handling() {
    return avro702Handling;
  }

  private void validateInput(Collection<File> files, String desc) {
    for (File f : files) {
      if (!f.exists()) {
        throw new IllegalArgumentException(desc + " path does not exist: " + f.getAbsolutePath());
      }
      if (!f.canRead()) {
        throw new IllegalArgumentException(desc + " path unreadable: " + f.getAbsolutePath());
      }
      if (!f.isFile() && !f.isDirectory()) {
        throw new IllegalArgumentException(desc + "path must be file or directory: " + f.getAbsolutePath());
      }
    }
  }

  private void validateOutput(File f, String desc) {
    if (f == null || !f.exists()) {
      return;
    }
    if (!f.isDirectory()) {
      throw new IllegalArgumentException(desc + " folder " + outputSpecificRecordClassesRoot.getAbsolutePath() + " is not a directory");
    }
    if (!f.canWrite()) {
      throw new IllegalArgumentException(desc + " folder " + outputSpecificRecordClassesRoot.getAbsolutePath() + " not writable");
    }
  }
}
