/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.exceptions;

/**
 * indicates the input was not valid avro according to the
 * <a href="http://avro.apache.org/docs/current/spec.html">avro specification</a>
 */
public class AvroSyntaxException extends ParseException {
    public AvroSyntaxException(String message) {
        super(message);
    }

    public AvroSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }
}
