/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Locale;

public abstract class AvroSchema {
    public abstract AvroType type();

    @Override
    public String toString() {
        return type().name().toLowerCase(Locale.ROOT);
    }
}
