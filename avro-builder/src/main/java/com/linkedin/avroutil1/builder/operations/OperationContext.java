/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations;

import java.util.ArrayList;
import java.util.List;


/**
 * common context to all {@link Operation}s run during an execution
 */
public class OperationContext {

  private List<Operation> operations = new ArrayList<>(1);
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
    //"seal" any internal state to prevent plugins from trying to do weird things during execution
    sealed = true;

    for (Operation op : operations) {
      op.run(this);
    }
  }
}
