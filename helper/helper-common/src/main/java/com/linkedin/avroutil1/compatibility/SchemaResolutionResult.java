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
    /**
     * true if promotion (widening int to long for example)
     * has to be used to make this reader to writer match
     */
    private final boolean writerPromoted;
    /**
     * true if the match was done via an alias on the reader schema
     */
    private final boolean readerAliasUsed;

    @Deprecated
    public SchemaResolutionResult(Schema readerMatch, Schema writerMatch, boolean writerPromoted) {
        this(readerMatch, writerMatch, writerPromoted, false);
    }

    public SchemaResolutionResult(
        Schema readerMatch,
        Schema writerMatch,
        boolean writerPromoted,
        boolean readerAliasUsed
    ) {
        this.readerMatch = readerMatch;
        this.writerMatch = writerMatch;
        this.writerPromoted = writerPromoted;
        this.readerAliasUsed = readerAliasUsed;
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
