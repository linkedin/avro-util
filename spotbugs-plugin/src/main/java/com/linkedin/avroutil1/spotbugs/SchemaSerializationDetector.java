/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import javax.annotation.Nonnull;
import org.apache.bcel.Const;


/***
 * Avro v1.4 is vulnerable to AVRO-702,
 * Detects and reports usage of org.apache.avro.Schema.toString
 * Also detects String.valueOf(org.apache.avro.Schema) which uses toString
 */
public class SchemaSerializationDetector extends OpcodeStackDetector {
  public static final String BUG_TYPE = "SCHEMA_SERIALIZATION_USING_TOSTRING";
  public static final String EMPTY_STRING = "";

  private final BugReporter bugReporter;

  public SchemaSerializationDetector(BugReporter bugReporter) {
    this.bugReporter = bugReporter;
  }

  @Override
  public void sawOpcode(int seen) {
    final String slashedSchemaClassPath = "org/apache/avro/Schema";
    final String slashedStringClassPath = "java/lang/String";
    final String toStringMethod = "toString";
    final String valueOfMethod = "valueOf";

    if (seen != Const.INVOKEVIRTUAL && seen != Const.INVOKESTATIC) {
      return;
    }

    if (toStringMethod.equals(getMethodDescriptorOperand().getName()) && checkClassConstantOperand(slashedSchemaClassPath)
        || valueOfMethod.equals(getMethodDescriptorOperand().getName()) && checkClassConstantOperand(
        slashedStringClassPath) && getStackItemSignature().contains(slashedSchemaClassPath)) {
      BugInstance bug =
          new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY).addClassAndMethod(this).addSourceLine(this, getPC());
      bugReporter.reportBug(bug);
    }
  }

  private boolean checkClassConstantOperand(@Nonnull String classPath) {
    return getClassConstantOperand() == null || classPath.equals(getClassConstantOperand());
  }

  private String getStackItemSignature() {
    return getStack() == null || getStack().getStackDepth() == 0 ? EMPTY_STRING : getStack().getStackItem(0).getSignature();
  }
}
