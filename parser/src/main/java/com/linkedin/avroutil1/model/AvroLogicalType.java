/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * all avro logical types. logical types are derived from {@link AvroType}s
 */
public enum AvroLogicalType {
    /**
     * arbitrary-precision signed decimal number
     */
    DECIMAL(AvroType.BYTES, AvroType.FIXED),
    /**
     * universally unique identifier, encoded as a RFC-4122 string
     */
    UUID(AvroType.STRING),
    /**
     * number of days from the unix epoch, 1 January 1970 (no TZ or time-of-day)
     */
    DATE(AvroType.INT),
    /**
     * number of milliseconds after midnight, 00:00:00.000 (no TZ or date)
     */
    TIME_MILLIS(AvroType.INT),
    /**
     * number of microseconds after midnight, 00:00:00.000 (no TZ or date)
     */
    TIME_MICROS(AvroType.LONG),
    /**
     * milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC
     */
    TIMESTAMP_MILLIS(AvroType.LONG),
    /**
     * number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC
     */
    TIMESTAMP_MICROS(AvroType.LONG),
    /**
     * number of milliseconds, from 1 January 1970 00:00:00.000 of the "local timezone"
     */
    LOCAL_TIMESTAMP_MILLIS(AvroType.LONG),
    /**
     * number of microseconds, from 1 January 1970 00:00:00.000000 of the "local timezone"
     */
    LOCAL_TIMESTAMP_MICROS(AvroType.LONG),
    /**
     * an amount of time defined by a number of months, days and milliseconds.
     * encoded as 12 bytes - three little-endian unsigned integers:
     * <ul>
     *     <li>number of months</li>
     *     <li>number of days</li>
     *     <li>number of milliseconds</li>
     * </ul>
     */
    DURATION(AvroType.FIXED);

    private final Set<AvroType> parentTypes;

    AvroLogicalType(AvroType... parents) {
        if (parents == null || parents.length == 0) {
            throw new IllegalArgumentException("logical type " + name() + " must have parent(s)");
        }
        Set<AvroType> parentSet = new HashSet<>(1);
        for (AvroType parent : parents) {
            if (!parentSet.add(parent)) {
                throw new IllegalArgumentException("duplicate value " + parent + " as allowed parent type for logical type " + name());
            }
        }
        this.parentTypes = Collections.unmodifiableSet(parentSet);
    }

    /**
     * @return the {@link AvroType}s this LogicalType could be a specialization of
     */
    public Set<AvroType> getParentTypes() {
        return parentTypes;
    }

    public static AvroLogicalType fromJson(String jsonLogicalTypeStr) {
        //todo - optimize to not rely on exception
        try {
            String transformed = jsonLogicalTypeStr.toUpperCase(Locale.ROOT).replaceAll("-", "_");
            return valueOf(transformed);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    @Override
    public String toString() {
        //match the avro spec name
        return name().replaceAll("_", "-").toLowerCase(Locale.ROOT);
    }
}
