/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.stream.JsonLocation;

public class JsonLocationImpl implements JsonLocation {
    private final long lineNumber;
    private final long columnNumber;
    private final long streamOffset;

    public JsonLocationImpl(long lineNumber, long columnNumber, long streamOffset) {
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
        this.streamOffset = streamOffset;
    }

    @Override
    public long getLineNumber() {
        return lineNumber;
    }

    @Override
    public long getColumnNumber() {
        return columnNumber;
    }

    @Override
    public long getStreamOffset() {
        return streamOffset;
    }

    @Override
    public String toString() {
        return "(line no=" + lineNumber + ", column no=" + columnNumber + ", offset=" +  streamOffset +")";
    }
}
