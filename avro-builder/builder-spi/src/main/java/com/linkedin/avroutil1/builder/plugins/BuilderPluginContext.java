/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.plugins;

import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.util.StreamUtil;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * context for running a set of {@link  com.linkedin.avroutil1.builder.plugins.BuilderPlugin}s
 */
public class BuilderPluginContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(BuilderPluginContext.class);

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

    int operationCount = operations.stream().collect(StreamUtil.toParallelStream(op -> {
      try {
        op.run(operationContext);
      } catch (Exception e) {
        throw new IllegalStateException("Exception running operation", e);
      }

      return 1;
    }, 2)).reduce(0, Integer::sum);

    LOGGER.info("Executed {} operations for builder plugins", operationCount);
  }
}
