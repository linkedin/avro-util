/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.plugins;

import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import java.util.ArrayList;
import java.util.List;


/**
 * context for running a set of {@link  com.linkedin.avroutil1.builder.plugins.BuilderPlugin}s
 */
public class BuilderPluginContext {

  private final List<Operation> operations = new ArrayList<>(1);
  private volatile boolean sealed = false;
  private final OperationContext operationContext;

  public BuilderPluginContext(OperationContext operationContext) {
    this.operationContext = operationContext;
  }

  public void add(Operation op) {
    if (sealed) {
      throw new IllegalStateException("configuration phase over, run() has been invoked");
    }
    if (op == null) {
      throw new IllegalArgumentException("op required");
    }
    operations.add(op);
  }

  public void run() throws Exception {
    if (sealed) {
      throw new IllegalStateException("run() has already been invoked");
    }

    //"seal" any internal state to prevent plugins from trying to do weird things during execution
    sealed = true;

    operations.parallelStream().forEach(op -> {
      try {
        op.run(operationContext);
      } catch (Exception e) {
        throw new IllegalStateException("Exception running operation", e);
      }
    });
  }
}
