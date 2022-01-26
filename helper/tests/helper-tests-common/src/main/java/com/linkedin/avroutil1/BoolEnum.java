/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;


/**
 * this class exists so we can have "-flag true" style boolean flags in args4j
 */
public enum BoolEnum {
    FALSE, TRUE;

    boolean get() {
        return this == TRUE;
    }
}
