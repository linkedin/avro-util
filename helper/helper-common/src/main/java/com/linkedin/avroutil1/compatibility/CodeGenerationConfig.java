/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Set;

/**
 * allow configuring the results of java code generation
 * from avro schemas
 */
public class CodeGenerationConfig {
    public static final CodeGenerationConfig COMPATIBLE_DEFAULTS = new CodeGenerationConfig(
            //TODO - in the future enable avro-702 mitigation here
            StringRepresentation.CharSequence
    );

    /**
     * how to represent string properties in generated code
     */
    private final StringRepresentation stringRepresentation;

    /**
     * if false no attempt to handle avro-702 (either way) will be made
     * and generated code will be at the mercy of runtime avro.
     */
    private final boolean enableAvro702Handling;

    /**
     * for schemas susceptible to avro-702 (and if {@link #enableAvro702Handling} is true)
     * what to replace the embedded SCHEMA$ with
     */
    private final AvscGenerationConfig avro702AvscReplacement;

    public CodeGenerationConfig(
            StringRepresentation stringRepresentation,
            boolean enableAvro702Handling,
            AvscGenerationConfig avro702AvscReplacement
    ) {
        this.stringRepresentation = stringRepresentation;
        this.enableAvro702Handling = enableAvro702Handling;
        this.avro702AvscReplacement = avro702AvscReplacement;
    }

    @Deprecated
    public CodeGenerationConfig(
            StringRepresentation stringRepresentation,
            boolean noAvro702Mitigation,
            Set<String> schemasToGenerateBadAvscFor
    ) {
        this.stringRepresentation = stringRepresentation;
        this.enableAvro702Handling = !noAvro702Mitigation;
        if (schemasToGenerateBadAvscFor != null && !schemasToGenerateBadAvscFor.isEmpty()) {
            throw new IllegalArgumentException("schemasToGenerateBadAvscFor no longer supported");
        }
        this.avro702AvscReplacement = this.enableAvro702Handling ?
                AvscGenerationConfig.CORRECT_MITIGATED_ONELINE : AvscGenerationConfig.VANILLA_ONELINE;
    }

    @Deprecated
    public CodeGenerationConfig(
            StringRepresentation stringRepresentation,
            Set<String> schemasToGenerateBadAvscFor
    ) {
        this(stringRepresentation, true, schemasToGenerateBadAvscFor);
    }

    @Deprecated
    public CodeGenerationConfig(StringRepresentation stringRepresentation) {
        this(stringRepresentation, false, AvscGenerationConfig.VANILLA_ONELINE);
    }

    public StringRepresentation getStringRepresentation() {
        return stringRepresentation;
    }

    public boolean isAvro702HandlingEnabled() {
        return enableAvro702Handling;
    }

    @Deprecated
    public boolean isNoAvro702Mitigation() {
        return !enableAvro702Handling;
    }

    @Deprecated
    public Set<String> getSchemasToGenerateBadAvscFor() {
        throw new UnsupportedOperationException("feature no longer supported");
    }

    public AvscGenerationConfig getAvro702AvscReplacement() {
        return avro702AvscReplacement;
    }
}
