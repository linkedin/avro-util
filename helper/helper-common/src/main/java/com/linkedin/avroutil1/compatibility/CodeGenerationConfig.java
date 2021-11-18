/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Collections;
import java.util.Set;

/**
 * allow configuring the results of java code generation
 * from avro schemas
 */
public class CodeGenerationConfig {
    public static final CodeGenerationConfig COMPATIBLE_DEFAULTS = new CodeGenerationConfig(StringRepresentation.CharSequence);

    /**
     * how to represent string properties in generated code
     */
    private final StringRepresentation stringRepresentation;

    /**
     * schema full names for which to use avro 1.4's bad logic to calculate the avsc for SCHEMA$
     * (see https://issues.apache.org/jira/browse/AVRO-702). this may be required for bug-to-bug
     * compatibility with legacy generated code
     */
    private final Set<String> schemasToGenerateBadAvscFor;

    public CodeGenerationConfig(StringRepresentation stringRepresentation, Set<String> schemasToGenerateBadAvscFor) {
        this.stringRepresentation = stringRepresentation;
        this.schemasToGenerateBadAvscFor = schemasToGenerateBadAvscFor;
    }

    @Deprecated
    public CodeGenerationConfig(StringRepresentation stringRepresentation) {
        this(stringRepresentation, Collections.emptySet());
    }

    public StringRepresentation getStringRepresentation() {
        return stringRepresentation;
    }

    public Set<String> getSchemasToGenerateBadAvscFor() {
        return schemasToGenerateBadAvscFor;
    }
}
