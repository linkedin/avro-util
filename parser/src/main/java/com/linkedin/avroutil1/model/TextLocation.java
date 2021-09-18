/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Objects;

/**
 * represents a single location in a body of text
 */
public class TextLocation {
    private final long lineNumber; //starts with 1
    private final long columnNumber; //starts with 1
    private final long streamOffset; //either characters or bytes, depending on context starts with 1

    public TextLocation(long lineNumber, long columnNumber, long streamOffset) {
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
        this.streamOffset = streamOffset;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public long getColumnNumber() {
        return columnNumber;
    }

    public long getStreamOffset() {
        return streamOffset;
    }

    @Override
    public String toString() {
        return "(line=" + lineNumber + ", column=" + columnNumber + ", offset=" +  streamOffset + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextLocation that = (TextLocation) o;
        return lineNumber == that.lineNumber && columnNumber == that.columnNumber && streamOffset == that.streamOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lineNumber, columnNumber, streamOffset);
    }
}
