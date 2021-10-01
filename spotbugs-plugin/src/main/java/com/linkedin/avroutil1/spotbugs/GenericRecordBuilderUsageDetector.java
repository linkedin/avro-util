/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugReporter;

/**
 * detects usage of org.apache.avro.generic.GenericRecordBuilder,
 * which only exists in avro 1.6+
 */
public class GenericRecordBuilderUsageDetector extends AbstractUsageDetector {

    public GenericRecordBuilderUsageDetector(BugReporter bugReporter) {
        super(bugReporter, "org.apache.avro.generic.GenericRecordBuilder", "GENERICRECORDBUILDER_USAGE");
    }
}
