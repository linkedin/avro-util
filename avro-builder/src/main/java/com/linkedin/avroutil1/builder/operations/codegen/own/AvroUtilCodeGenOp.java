/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.parser.avsc.AvroParseContext;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    //mkdir any output folders that dont exist
    if (!config.getOutputSpecificRecordClassesRoot().exists() && !config.getOutputSpecificRecordClassesRoot().mkdirs()) {
      throw new IllegalStateException("unable to create destination folder " + config.getOutputSpecificRecordClassesRoot());
    }

    List<Path> avscFiles = new ArrayList<>();
    for (File inputRoot : config.getInputRoots()) {
      Files.walk(inputRoot.toPath())
          .filter(path -> path.getFileName().toString().endsWith("." + BuilderConsts.AVSC_EXTENSION))
          .forEach(avscFiles::add);
    }
    if (avscFiles.isEmpty()) {
      LOGGER.warn("no input schema files were found under roots " + config.getInputRoots());
      return;
    }
    LOGGER.info("found " + avscFiles.size() + " avsc schema files");

    AvroParseContext context = new AvroParseContext();
    AvscParser parser = new AvscParser();

    for (Path p : avscFiles) {
      AvscParseResult fileParseResult = parser.parse(p);
      Throwable parseError = fileParseResult.getParseError();
      if (parseError != null) {
        throw new IllegalArgumentException("failed to parse file " + p.toAbsolutePath(), parseError);
      }
      context.add(fileParseResult);
    }

    //resolve any references across files that are part of this op (anything left would be external)
    context.resolveReferences();

    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException("unresolved referenced to external schemas: " + context.getExternalReferences());
    }

    //TODO - look for dups

    throw new UnsupportedOperationException("TBD");
  }
}
