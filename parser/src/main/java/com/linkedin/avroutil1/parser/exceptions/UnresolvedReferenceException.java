/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.exceptions;

public class UnresolvedReferenceException extends ParseException {

    public UnresolvedReferenceException(String message) {
        super(message);
    }
}
