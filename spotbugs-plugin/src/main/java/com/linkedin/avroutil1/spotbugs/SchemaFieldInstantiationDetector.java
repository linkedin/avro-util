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
 * detects direct instantiations of Schema.Field, the constructor of which
 * has changed in avro 1.8 and again incompatibly in avro 1.9+
 */
public class SchemaFieldInstantiationDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;

    public SchemaFieldInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKESPECIAL) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/Schema$Field") &&
                getMethodDescriptorOperand().getName().equals("<init>")
        ) {
            // constructor call for Field
            BugInstance bug = new BugInstance(this, "SCHEMAFIELD_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
