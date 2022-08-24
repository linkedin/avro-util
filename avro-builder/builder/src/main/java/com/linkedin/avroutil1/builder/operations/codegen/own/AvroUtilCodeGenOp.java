/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.OperationContext;
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
import java.util.Map;
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
    Set<File> nonImportableFiles = new HashSet<>();
    String[] extensions = new String[]{BuilderConsts.AVSC_EXTENSION};
    long scanStart = System.currentTimeMillis();

    for (File inputRoot : config.getInputRoots()) {
      avscFiles.addAll(FileUtils.listFiles(inputRoot, extensions, true));
    }

    if (config.getNonImportableSourceRoots() != null) {
      for (File nonImportableRoot : config.getNonImportableSourceRoots()) {
        nonImportableFiles.addAll(FileUtils.listFiles(nonImportableRoot, extensions, true));
      }
    }

    long scanEnd = System.currentTimeMillis();

    int numFiles = avscFiles.size() + nonImportableFiles.size();
    if (numFiles == 0) {
      LOGGER.warn("no input schema files were found under roots " + config.getInputRoots());
      return;
    }
    LOGGER.info("found " + numFiles + " avsc schema files in " + (scanEnd - scanStart) + " millis");

    AvroParseContext context = new AvroParseContext();
    Set<AvscParseResult> parsedFiles = new HashSet<>();

    parsedFiles.addAll(parseAvscFiles(avscFiles, true, context));
    parsedFiles.addAll(parseAvscFiles(nonImportableFiles, false, context));

    //resolve any references across files that are part of this op (anything left would be external)
    context.resolveReferences();

    long parseEnd = System.currentTimeMillis();

    Map<String, AvscParseResult> allNamedSchemas = context.getAllNamedSchemas();
    Map<String, List<AvscParseResult>> duplicates = context.getDuplicates();

    LOGGER.info("parsed {} named schemas in {} millis, {} of which have duplicates",
        allNamedSchemas.size(), parseEnd - scanEnd, duplicates.size());

    //TODO fail if any errors or dups (depending on config) are found
    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException(
          "unresolved referenced to external schemas: " + context.getExternalReferences());
    }

    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    List<JavaFileObject> specificRecords = new ArrayList<>(allNamedSchemas.size());

    long genStart = System.currentTimeMillis();
    long errorCount = 0;
    for (Map.Entry<String, AvscParseResult> namedSchemaEntry : allNamedSchemas.entrySet()) {
      String fullname = namedSchemaEntry.getKey();
      AvscParseResult fileParseResult = namedSchemaEntry.getValue();
      AvroNamedSchema namedSchema = fileParseResult.getDefinedSchema(fullname);

      try {
        List<JavaFileObject> javaFileObjects = generator.generateSpecificClass(
            namedSchema,
            SpecificRecordGenerationConfig.BROAD_COMPATIBILITY
        );
        specificRecords.addAll(javaFileObjects);
      } catch (Exception e) {
        errorCount++;
        //TODO - error-out
        LOGGER.error("failed to generate class for " + fullname + " defined in " + fileParseResult.getContext().getUri(), e);
      }
    }
    long genEnd = System.currentTimeMillis();

    if (errorCount > 0) {
      LOGGER.info("failed to generate {} java source files ({} generated successfully) in {} millis",
          errorCount, specificRecords.size(), genEnd - genStart);
    } else {
      LOGGER.info("generated {} java source files in {} millis", specificRecords.size(), genEnd - genStart);
    }

    writeJavaFilesToDisk(specificRecords, config.getOutputSpecificRecordClassesRoot());

    long writeEnd = System.currentTimeMillis();
    LOGGER.info("wrote out {} generated java source files under {} in {} millis",
        specificRecords.size(), config.getOutputSpecificRecordClassesRoot(), writeEnd - genEnd);

    Set<File> allAvroFiles = new HashSet<>(avscFiles);
    allAvroFiles.addAll(nonImportableFiles);
    opContext.addParsedSchemas(context.getTopLevelSchemas(), allAvroFiles);
  }

  /**
   * A helper to parse avsc files.
   *
   * @param avscFiles Avsc files to parse
   * @param areFilesImportable whether to allow other avsc files to import from this avsc file
   * @param context the full parsing context for this "run".
   * @return a set of all the parsed file results (a Set of AvscParseResult)
   */
  private Set<AvscParseResult> parseAvscFiles(Set<File> avscFiles, boolean areFilesImportable,
      AvroParseContext context) {
    AvscParser parser = new AvscParser();
    HashSet<AvscParseResult> parsedFiles = new HashSet<>();
    for (File p : avscFiles) {
      AvscParseResult fileParseResult = parser.parse(p);
      Throwable parseError = fileParseResult.getParseError();
      if (parseError != null) {
        throw new IllegalArgumentException("failed to parse file " + p.getAbsolutePath(), parseError);
      }
      context.add(fileParseResult, areFilesImportable);
      parsedFiles.add(fileParseResult);
    }
    return parsedFiles;
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

      try (
          FileOutputStream fos = new FileOutputStream(outputFile, false);
          OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
          Reader reader = javaClass.openReader(true)
      ) {
        IOUtils.copy(reader, writer);
        writer.flush();
        fos.flush();
      } catch (Exception e) {
        throw new IllegalStateException("while writing file " + outputFile, e);
      }
    }
  }
}
