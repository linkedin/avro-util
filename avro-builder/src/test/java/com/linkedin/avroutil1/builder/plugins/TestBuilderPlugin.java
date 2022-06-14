/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.plugins;

import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


public class TestBuilderPlugin implements BuilderPlugin {
  private OptionSpec<String> testOption = null;
  private File extraFile = null;

  @Override
  public String name() {
    return "TestPlugin";
  }

  @Override
  public Set<Integer> supportedApiVersions() {
    return Collections.singleton(1);
  }

  @Override
  public void customizeCLI(OptionParser parser) {
    if (testOption != null) {
      throw new IllegalStateException();
    }
    testOption = parser.accepts("extraFileName", "added by " + name()).withOptionalArg();
  }

  @Override
  public void parseAndValidateOptions(OptionSet options) {
    if (testOption == null) {
      throw new IllegalStateException();
    }
    if (options.has(testOption)) {
      extraFile = new File(options.valueOf(testOption));
    } else {
      extraFile = null;
    }
  }

  @Override
  public void createOperations(OperationContext context) {
    if (extraFile != null) {
      context.add(new CreateDummyFileOperation(extraFile));
    }
  }

  private static class CreateDummyFileOperation implements Operation {
    private final File file;

    public CreateDummyFileOperation(File file) {
      if (file == null) {
        throw new IllegalArgumentException();
      }
      this.file = file;
    }

    @Override
    public void run(OperationContext opContext) throws Exception {
      try (FileWriter writer = new FileWriter(file, false)) {
        writer.write("this is a dummy file");
      }
    }
  }
}
