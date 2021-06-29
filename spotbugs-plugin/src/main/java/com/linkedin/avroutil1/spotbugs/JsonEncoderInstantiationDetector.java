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
 * detects direct instantiations of JsonEncoder
 */
public class JsonEncoderInstantiationDetector extends OpcodeStackDetector {
    private final BugReporter bugReporter;

    public JsonEncoderInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.NEW) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/io/JsonEncoder")) {
            // new JsonEncoder call
            BugInstance bug = new BugInstance(this, "JSON_ENCODER_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
