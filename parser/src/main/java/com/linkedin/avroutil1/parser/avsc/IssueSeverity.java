/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

public enum IssueSeverity {
    /**
     * prevents parsing at least parts of a file. does not produce a working schema.
     * examples - malformed json
     */
    SEVERE,
    /**
     * does not conform to the avro specification, yet could be accepted and used
     * (at least to some extent) by some more lenient implementations.
     * examples - bad default value, spaces in field names
     */
    SPEC_VIOLATION,
    /**
     * legal, strictly speaking, but might cause your fellow developers to come
     * after you with sharp pointy objects
     * examples - use of full names, fields with the same case-insensitive name
     */
    WARNING
}
