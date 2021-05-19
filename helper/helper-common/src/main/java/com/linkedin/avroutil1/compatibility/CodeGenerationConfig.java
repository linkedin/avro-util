/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

/**
 * allow configuring the results of java code generation
 * from avro schemas
 */
public class CodeGenerationConfig {
    public static final CodeGenerationConfig COMPATIBLE_DEFAULTS = new CodeGenerationConfig(StringRepresentation.CharSequence);

    private final StringRepresentation stringRepresentation;

    public CodeGenerationConfig(StringRepresentation stringRepresentation) {
        this.stringRepresentation = stringRepresentation;
    }

    public StringRepresentation getStringRepresentation() {
        return stringRepresentation;
    }
}
