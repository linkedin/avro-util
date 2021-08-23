/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.net.URI;

/**
 * represents the location of a piece of "code" (usually a schema).
 * composed of a URI representing the source url/file and start/end
 * positions therein
 */
public class CodeLocation {
    private final URI uri;
    private final TextLocation start;
    private final TextLocation end;

    public CodeLocation(URI uri, TextLocation start, TextLocation end) {
        this.uri = uri;
        this.start = start;
        this.end = end;
    }

    public URI getUri() {
        return uri;
    }

    public TextLocation getStart() {
        return start;
    }

    public TextLocation getEnd() {
        return end;
    }
}
