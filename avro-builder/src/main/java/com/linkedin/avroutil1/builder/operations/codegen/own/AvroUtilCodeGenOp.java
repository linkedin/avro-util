/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avro.codegen.CodeGenerator;
import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
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
  private final CodeGenerator generator = new CodeGenerator();

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
    generator.setInputs(config.getInputRoots());

    generator.setOutputFolder(config.getOutputSpecificRecordClassesRoot());

    generator.generateCode();
  }
}
