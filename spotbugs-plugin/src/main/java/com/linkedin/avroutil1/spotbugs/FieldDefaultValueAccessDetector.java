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

/**
 * detects assess to org.apache.avro.Schema#Field default values
 */
public class FieldDefaultValueAccessDetector extends OpcodeStackDetector {
    private final BugReporter bugReporter;

    public FieldDefaultValueAccessDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKEVIRTUAL) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/Schema$Field") &&
                (getMethodDescriptorOperand().getName().equals("defaultValue")
                        || getMethodDescriptorOperand().getName().equals("defaultVal"))
        ) {
            // new BinaryEncoder call
            BugInstance bug = new BugInstance(this, "FIELD_DEFAULT_VALUE_ACCESS", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
