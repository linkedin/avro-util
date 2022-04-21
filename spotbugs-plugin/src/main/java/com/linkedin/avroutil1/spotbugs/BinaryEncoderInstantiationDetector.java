/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import org.apache.bcel.Const;

/**
 * detects direct instantiations of BinaryEncoder
 */
public class BinaryEncoderInstantiationDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;

    public BinaryEncoderInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.NEW) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/io/BinaryEncoder")) {
            // new BinaryEncoder call
            BugInstance bug = new BugInstance(this, "BINARY_ENCODER_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
