/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen;

import com.beust.jcommander.JCommander;


public class Tool {

  public static void main(String[] args) {
    ToolArgs conf = parseArgs(args);

    CodeGenerator codegen = new CodeGenerator();
    codegen.setInputs(conf.getSrc());
    codegen.setOutputFolder(conf.getOutputFolder());
    codegen.setIncludes(conf.getIncludes());
    codegen.setAllowClasspathLookup(conf.isAllowClasspathLookup());
    codegen.setAllowReverseEngineeringClasspathSchemas(conf.isAllowReverseEngineeringClasspathSchemas());
    codegen.setValidateSchemaNamespaceVsFilePath(conf.isValidateSchemaNamespaceVsFilePath());
    codegen.setValidateSchemaNameVsFileName(conf.isValidateSchemaNameVsFileName());

    codegen.generateCode();
  }

  private static ToolArgs parseArgs(String[] args) {
    ToolArgs parsed = new ToolArgs();
    JCommander.newBuilder()
        .addObject(parsed)
        .build()
        .parse(args);
    return parsed;
  }
}
