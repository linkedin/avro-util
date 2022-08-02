/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.codegen.SpecificRecordClassGenerator;
import com.linkedin.avroutil1.codegen.SpecificRecordGenerationConfig;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.parser.avsc.AvroParseContext;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.tools.JavaFileObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a code generation operation using the avro-codegen module of avro-util
 */
public class AvroUtilCodeGenOp implements Operation {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtilCodeGenOp.class);

  private final CodeGenOpConfig config;

  public AvroUtilCodeGenOp(CodeGenOpConfig config) {
    this.config = config;
  }

  @Override
  public void run(OperationContext opContext) throws Exception {
    //mkdir any output folders that don't exist
    if (!config.getOutputSpecificRecordClassesRoot().exists() && !config.getOutputSpecificRecordClassesRoot()
        .mkdirs()) {
      throw new IllegalStateException(
          "unable to create destination folder " + config.getOutputSpecificRecordClassesRoot());
    }

    Set<File> avscFiles = new HashSet<>();
    Set<File> importableFiles = new HashSet<>();
    String[] extensions = new String[]{BuilderConsts.AVSC_EXTENSION};

    for (File inputRoot : config.getInputRoots()) {
      avscFiles.addAll(FileUtils.listFiles(inputRoot, extensions, true));
    }

    if (config.getIncludeRoots() != null) {
      for (File includeRoot : config.getIncludeRoots()) {
        importableFiles.addAll(FileUtils.listFiles(includeRoot, extensions, true));
      }
    }

    // If there are no importable files, then all of avscFiles should be marked as importable.
    boolean areAvscFilesImportable = importableFiles.isEmpty();

    if (avscFiles.isEmpty() && importableFiles.isEmpty()) {
      LOGGER.warn("no input schema files were found under roots " + config.getInputRoots());
      return;
    }
    LOGGER.info("found " + (avscFiles.size() + importableFiles.size()) + " avsc schema files");

    AvroParseContext context = new AvroParseContext();
    List<AvscParseResult> parsedFiles = new ArrayList<>();

    parseAvscFiles(importableFiles, true, context, parsedFiles);
    parseAvscFiles(avscFiles, areAvscFilesImportable, context, parsedFiles);

    //resolve any references across files that are part of this op (anything left would be external)
    context.resolveReferences();

    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException(
          "unresolved referenced to external schemas: " + context.getExternalReferences());
    }

    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    List<JavaFileObject> specificRecords = new ArrayList<>();
    for (AvscParseResult parsedFile : parsedFiles) {
      JavaFileObject javaFileObject =
          generator.generateSpecificRecordClass((AvroNamedSchema) parsedFile.getTopLevelSchema(),
              SpecificRecordGenerationConfig.BROAD_COMPATIBILITY);
      specificRecords.add(javaFileObject);
    }

    writeJavaFilesToDisk(specificRecords, config.getOutputSpecificRecordClassesRoot());

    //TODO - look for dups

  }

  private void parseAvscFiles(Set<File> avscFiles, boolean areFilesImportable, AvroParseContext context,
      List<AvscParseResult> parsedFiles) {
    AvscParser parser = new AvscParser();
    for (File p : avscFiles) {
      AvscParseResult fileParseResult = parser.parse(p);
      Throwable parseError = fileParseResult.getParseError();
      if (parseError != null) {
        throw new IllegalArgumentException("failed to parse file " + p.getAbsolutePath(), parseError);
      }
      context.add(fileParseResult, areFilesImportable);
      parsedFiles.add(fileParseResult);
    }
  }

  private void writeJavaFilesToDisk(Collection<JavaFileObject> javaClassFiles, File outputFolder) {
    //make sure the output folder exists
    if (!outputFolder.exists() && !outputFolder.mkdirs()) {
      throw new IllegalStateException("unable to create output folder " + outputFolder);
    }

    //write out the files we generated
    for (JavaFileObject javaClass : javaClassFiles) {
      File outputFile = new File(outputFolder, javaClass.getName());

      if (outputFile.exists()) {
        //TODO - make this behaviour configurable (overwite, ignore, ignore_if_identical, etc)
        throw new IllegalStateException("output file " + outputFile + " already exists");
      } else {
        File parentFolder = outputFile.getParentFile();
        if (!parentFolder.exists() && !parentFolder.mkdirs()) {
          throw new IllegalStateException("unable to create output folder " + outputFolder);
        }
      }

      try (FileOutputStream fos = new FileOutputStream(outputFile, false);
          OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
          Reader reader = javaClass.openReader(true)) {
        IOUtils.copy(reader, writer);
        writer.flush();
        fos.flush();
      } catch (Exception e) {
        throw new IllegalStateException("while writing file " + outputFile, e);
      }
    }
  }
}
