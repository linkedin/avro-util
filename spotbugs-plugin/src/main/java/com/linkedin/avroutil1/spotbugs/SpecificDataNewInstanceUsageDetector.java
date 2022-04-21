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
 * detects calls to SpecificData.newInstance(), which may not work as expected
 * in the presence of old (pre 1.6) SchemaConstructables
 */
public class SpecificDataNewInstanceUsageDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;

    public SpecificDataNewInstanceUsageDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKESTATIC) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/specific/SpecificData") && getMethodDescriptorOperand().getName().equals("newInstance")) {
            // SpecificData.newInstance(...)
            BugInstance bug = new BugInstance(this, "SPECIFICDATA_NEWINSTANCE_USAGE", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
