/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import org.apache.bcel.Const;

public class JsonDecoderInstantiationDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;

    public JsonDecoderInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.NEW) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/io/JsonDecoder")) {
            // new JsonDecoder call
            BugInstance bug = new BugInstance(this, "JSON_DECODER_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}