/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugReporter;

/**
 * detects usage of org.apache.avro.message.BinaryMessageEncoder,
 * which only exists in avro 1.8+
 */
public class BinaryMessageEncoderUsageDetector extends AbstractUsageDetector {

    public BinaryMessageEncoderUsageDetector(BugReporter bugReporter) {
        super(bugReporter, "org.apache.avro.message.BinaryMessageEncoder", "BINARYMESSAGEENCODER_USAGE");
    }
}
