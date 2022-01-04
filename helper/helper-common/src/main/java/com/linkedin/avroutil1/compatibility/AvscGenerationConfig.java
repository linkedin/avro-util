/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

import java.util.Optional;

/**
 * config class packing various configuration parameters and settings for the generation of
 * avsc (json) strings out of {@link org.apache.avro.Schema} objects
 */
public class AvscGenerationConfig {
    /**
     * always punts to runtime avro to produce pretty output.
     * result might suffer from @see <a href="https://issues.apache.org/jira/browse/AVRO-702">AVRO-702</a> under 1.4
     * and also from incorrectly escaped json under versions smaller than 1.6
     * (@see <a href="https://issues.apache.org/jira/browse/AVRO-886">AVRO-886</a>)
     */
    public static final AvscGenerationConfig VANILLA_PRETTY = new AvscGenerationConfig(
            true, true, true, Optional.empty(), false
    );
    /**
     * always punts to runtime avro to produce terse output.
     * result might suffer from @see <a href="https://issues.apache.org/jira/browse/AVRO-702">AVRO-702</a> under 1.4
     * and also from incorrectly escaped json under versions smaller than 1.6
     * (@see <a href="https://issues.apache.org/jira/browse/AVRO-886">AVRO-886</a>)
     */
    public static final AvscGenerationConfig VANILLA_ONELINE  = new AvscGenerationConfig(
            true, false, false, Optional.empty(), false
    );
    /**
     * always generates correct, pretty-printed avsc. delegates this to runtime avro where possible
     */
    public static final AvscGenerationConfig CORRECT_PRETTY = new AvscGenerationConfig(
            true, false, true, Optional.of(Boolean.FALSE), false
    );
    /**
     * always generates correct, one-line avsc. delegates this to runtime avro where possible
     */
    public static final AvscGenerationConfig CORRECT_ONELINE = new AvscGenerationConfig(
            true, false, true, Optional.of(Boolean.FALSE), false
    );
    /**
     * always generates correct, pretty avsc, with aliases fo "bad" fullnames for better compatibility with avro 1.4
     */
    public static final AvscGenerationConfig CORRECT_MITIGATED_PRETTY = new AvscGenerationConfig(
            false, false, true, Optional.of(Boolean.FALSE), true
    );
    /**
     * always generates correct, one-line avsc, with aliases fo "bad" fullnames for better compatibility with avro 1.4
     */
    public static final AvscGenerationConfig CORRECT_MITIGATED_ONELINE = new AvscGenerationConfig(
            false, false, false, Optional.of(Boolean.FALSE), true
    );
    /**
     * always generates avro-702 impacted (but pretty) avsc. only use if you know what you're doing.
     */
    public static final AvscGenerationConfig LEGACY_PRETTY = new AvscGenerationConfig(
            false, false, true, Optional.of(Boolean.TRUE), false
    );
    /**
     * always generates avro-702 impacted, terse avsc. only use if you know what you're doing.
     */
    public static final AvscGenerationConfig LEGACY_ONELINE = new AvscGenerationConfig(
            false, false, false, Optional.of(Boolean.TRUE), false
    );
    /**
     * always generates avro-702 impacted (but pretty) avsc, and adds aliases to the correct full names of
     * impacted named types. only use if you know what you're doing.
     */
    public static final AvscGenerationConfig LEGACY_MITIGATED_PRETTY = new AvscGenerationConfig(
            false, false, true, Optional.of(Boolean.TRUE), true
    );
    /**
     * always generates avro-702 impacted (but pretty) avsc, and adds aliases to the correct full names of
     * impacted named types. only use if you know what you're doing.
     */
    public static final AvscGenerationConfig LEGACY_MITIGATED_ONELINE = new AvscGenerationConfig(
            false, false, false, Optional.of(Boolean.TRUE), true
    );

    /**
     * if this value is set to true, and the rest of the values on this config object
     * are within runtime avro's capabilities, delegates the work to {@link Schema#toString()}.
     * otherwise uses our own avsc generation code.
     * ignored if {@link #forceUseOfRuntimeAvro} is set
     */
    private final boolean preferUseOfRuntimeAvro;
    /**
     * similar to {@link #preferUseOfRuntimeAvro}, but instead throws an exception if its
     * not possible to delegate the work to vanilla runtime avro
     */
    private final boolean forceUseOfRuntimeAvro;
    /**
     * if true produces nicely indented json. otherwise produces a single-line json
     * with no newlines or spaces for indentation
     */
    private final boolean prettyPrint;
    /**
     * true to produce potentially bad avsc for compatibility with avro 1.4 output (under any runtime avro version)
     * false to produce proper, correct, avsc (again under any runtime avro version)
     * null to comply with the behaviour of runtime avro (which is bad under 1.4)
     * @see <a href="https://issues.apache.org/jira/browse/AVRO-702">AVRO-702</a>
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Boolean> retainPreAvro702Logic;
    /**
     * true adds aliases to all named types (record, enum, fixed) to their "other fullname".
     * "other fullname" is either the correct one of the avro702-impacted one, depending
     * on the value of {@link #retainPreAvro702Logic}.
     * it is useful to have aliases to these "other fullnames" on schemas in scenarios
     * where application code is in the process of migrating away from avro 1.4 but has to
     * do so gradually and without compatibility breaks or global coordinated deployments.
     */
    private final boolean addAvro702Aliases;

    public AvscGenerationConfig(
            boolean preferUseOfRuntimeAvro,
            boolean forceUseOfRuntimeAvro,
            boolean prettyPrint,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
            Optional<Boolean> retainPreAvro702Logic,
            boolean addAvro702Aliases
    ) {
        //noinspection OptionalAssignedToNull
        if (retainPreAvro702Logic == null) {
            throw new IllegalArgumentException("retainPreAvro702Logic cannot be null");
        }
        this.preferUseOfRuntimeAvro = preferUseOfRuntimeAvro;
        this.forceUseOfRuntimeAvro = forceUseOfRuntimeAvro;
        this.prettyPrint = prettyPrint;
        this.retainPreAvro702Logic = retainPreAvro702Logic;
        this.addAvro702Aliases = addAvro702Aliases;
    }

    public boolean isPreferUseOfRuntimeAvro() {
        return preferUseOfRuntimeAvro;
    }

    public boolean isForceUseOfRuntimeAvro() {
        return forceUseOfRuntimeAvro;
    }

    public boolean isPrettyPrint() {
        return prettyPrint;
    }

    public Optional<Boolean> getRetainPreAvro702Logic() {
        return retainPreAvro702Logic;
    }

    public boolean isAddAvro702Aliases() {
        return addAvro702Aliases;
    }
}
