/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations;

import com.linkedin.avroutil1.builder.operations.codegen.OperationContext;
import java.util.ArrayList;
import java.util.List;


/**
 * context for running a set of {@link  com.linkedin.avroutil1.builder.plugins.BuilderPlugin}s
 */
public class BuilderPluginContext {

  private List<Operation> operations = new ArrayList<>(1);
  private OperationContext _operationContext = new OperationContext();
  private volatile boolean sealed = false;

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

    for (Operation op : operations) {
      op.run(_operationContext);
    }
  }
}
