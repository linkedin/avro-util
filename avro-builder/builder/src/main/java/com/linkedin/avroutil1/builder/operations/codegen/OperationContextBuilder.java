/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen;

import com.linkedin.avroutil1.builder.operations.OperationContext;


/**
 * Builds operation context.
 */
public interface OperationContextBuilder {

  /**
   * Builds and returns the {@link OperationContext}.
   */
  OperationContext buildOperationContext(CodeGenOpConfig opConfig) throws Exception;
}
