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
 * detects direct instantiations of GenericData.Fixed, the constructor of which
 * has changed in 1.5+
 */
public class FixedInstantiationDetector extends OpcodeStackDetector {
    private final BugReporter bugReporter;

    public FixedInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKESPECIAL) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/generic/GenericData$Fixed") &&
                getMethodDescriptorOperand().getName().equals("<init>")
        ) {
            // constructor call for Fixed
            BugInstance bug = new BugInstance(this, "FIXED_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
