/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugReporter;

/**
 * detects usage of org.apache.avro.message.BinaryMessageDecoder,
 * which only exists in avro 1.8+
 */
public class BinaryMessageDecoderUsageDetector extends AbstractUsageDetector {

    public BinaryMessageDecoderUsageDetector(BugReporter bugReporter) {
        super(bugReporter, "org.apache.avro.message.BinaryMessageDecoder", "BINARYMESSAGEDECODER_USAGE");
    }
}
