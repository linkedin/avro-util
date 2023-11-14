/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.exception;

import com.linkedin.avroutil1.compatibility.avropath.AvroPath;

/**
 * thrown to indicate there are multiple conflicting schema definitions
 * within the same object graph or group of generated specific record classes
 */
public class InconsistentSchemaException extends Exception {
    /**
     * indicates the path to the location of the conflicting schema definition, from the
     * "root" (outer-most) object or schema.
     */
    private final AvroPath path;

    public InconsistentSchemaException(AvroPath path, String msg) {
        super(msg);
        this.path = path;
    }

    public AvroPath getPath() {
        return path;
    }
}
