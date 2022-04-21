/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

public class SchemaResolutionResult {
    private final Schema readerMatch;
    private final Schema writerMatch;
    private final boolean writerPromoted;

    public SchemaResolutionResult(Schema readerMatch, Schema writerMatch, boolean writerPromoted) {
        this.readerMatch = readerMatch;
        this.writerMatch = writerMatch;
        this.writerPromoted = writerPromoted;
    }

    public Schema getReaderMatch() {
        return readerMatch;
    }

    public Schema getWriterMatch() {
        return writerMatch;
    }

    public boolean isWriterPromoted() {
        return writerPromoted;
    }
}
