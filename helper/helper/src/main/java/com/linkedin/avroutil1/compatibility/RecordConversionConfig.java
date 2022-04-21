/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

/**
 * configuration for use in specific &lt;--&gt; generic record conversion operations
 */
public class RecordConversionConfig {
    public static final RecordConversionConfig ALLOW_ALL = new RecordConversionConfig(
        true,
        true,
        false,
        true,
        StringRepresentation.Utf8,
        false
    );

    /**
     * true to allow matching writer (source) to reader (destination) schema
     * by aliases ON THE READER SCHEMA if no exact match by fullname is found.
     * applies when converting named types
     */
    private final boolean useAliasesOnNamedTypes;
    /**
     * true to allow matching writer (source) to reader (destination) fields
     * by aliases on the READER SCHEMA fields if no exact match by field name
     * is found. applies only when converting the contents of record types
     */
    private final boolean useAliasesOnFields;
    /**
     * if aliases of any form are used, validates that only a single alias
     * matches and no ambiguity is possible
     */
    private final boolean validateAliasUniqueness;
    /**
     * true to supplement reader schema defaults when a writer's enum symbol
     * does not exist in the reader's schema (if a default value exists)
     */
    private final boolean useEnumDefaults;
    /**
     * type used for string values, if no other constraints
     */
    private final StringRepresentation preferredStringRepresentation;
    /**
     * adjusts string type according to "avro.java.string" property on generics
     * of the declared field types on specific class properties.
     * note that fields types are still honored if it is impossible to use the preferred representation
     */
    private final boolean useStringRepresentationHints;

    public RecordConversionConfig(
        boolean useAliasesOnNamedTypes,
        boolean useAliasesOnFields,
        boolean validateAliasUniqueness,
        boolean useEnumDefaults,
        StringRepresentation preferredStringRepresentation,
        boolean useStringRepresentationHints
    ) {
        this.useAliasesOnNamedTypes = useAliasesOnNamedTypes;
        this.useAliasesOnFields = useAliasesOnFields;
        this.validateAliasUniqueness = validateAliasUniqueness;
        this.useEnumDefaults = useEnumDefaults;
        this.preferredStringRepresentation = preferredStringRepresentation;
        this.useStringRepresentationHints = useStringRepresentationHints;
    }

    public boolean isUseAliasesOnNamedTypes() {
        return useAliasesOnNamedTypes;
    }

    public boolean isUseAliasesOnFields() {
        return useAliasesOnFields;
    }

    public boolean isValidateAliasUniqueness() {
        return validateAliasUniqueness;
    }

    public boolean isUseEnumDefaults() {
        return useEnumDefaults;
    }

    public StringRepresentation getPreferredStringRepresentation() {
        return preferredStringRepresentation;
    }

    public boolean isUseStringRepresentationHints() {
        return useStringRepresentationHints;
    }
}
