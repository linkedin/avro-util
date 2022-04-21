/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import org.apache.bcel.Const;

public class InstanceofGenericRecordDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;

    public InstanceofGenericRecordDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INSTANCEOF) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/generic/GenericRecord")) {
            // instanceof GenericRecord
            BugInstance bug = new BugInstance(this, "INSTANCEOF_GENERICRECORD", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
