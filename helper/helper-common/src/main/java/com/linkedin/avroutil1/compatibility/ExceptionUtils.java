/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

public class ExceptionUtils {

    private ExceptionUtils() {
        //util class
    }

    public static Throwable rootCause(Throwable throwable) {
        if (throwable == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        Throwable cause = throwable;
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
