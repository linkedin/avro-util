/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import org.apache.bcel.Const;


/***
 * Avro v1.4 is vulnerable to AVRO-702,
 * Detects and reports usage of org.apache.avro.Schema.toString
 */
public class SchemaSerializationDetector extends OpcodeStackDetector {
  private final BugReporter bugReporter;

  public SchemaSerializationDetector(BugReporter bugReporter) {
    this.bugReporter = bugReporter;
  }

  @Override
  public void sawOpcode(int seen) {
    if (seen != Const.INVOKEVIRTUAL) {
      return;
    }
    if (getClassConstantOperand().equals("org/apache/avro/Schema") && getMethodDescriptorOperand().getName()
        .equals("toString")) {
      {
        BugInstance bug =
            new BugInstance(this, "SCHEMA_SERIALIZATION_USING_TOSTRING", NORMAL_PRIORITY).addClassAndMethod(this)
                .addSourceLine(this, getPC());
        bugReporter.reportBug(bug);
      }
    }
  }
}
