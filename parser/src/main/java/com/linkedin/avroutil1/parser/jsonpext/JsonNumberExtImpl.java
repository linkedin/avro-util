/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.stream.JsonLocation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;

public class JsonNumberExtImpl extends JsonValueExtImpl implements JsonNumberExt {
    private final BigDecimal bigDecimal;

    public JsonNumberExtImpl(
            Path source,
            JsonLocation startLocation,
            JsonLocation endLocation,
            BigDecimal value
    ) {
        super(source, startLocation, endLocation);
        this.bigDecimal = value;
    }

    @Override
    public boolean isIntegral() {
        return bigDecimal.scale() == 0;
    }

    @Override
    public int intValue() {
        return bigDecimal.intValue();
    }

    @Override
    public int intValueExact() {
        return bigDecimal.intValueExact();
    }

    @Override
    public long longValue() {
        return bigDecimal.longValue();
    }

    @Override
    public long longValueExact() {
        return bigDecimal.longValueExact();
    }

    @Override
    public BigInteger bigIntegerValue() {
        return bigDecimal.toBigInteger();
    }

    @Override
    public BigInteger bigIntegerValueExact() {
        return bigDecimal.toBigIntegerExact();
    }

    @Override
    public double doubleValue() {
        return bigDecimal.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return bigDecimal;
    }

    @Override
    public Number numberValue() {
        return bigDecimal;
    }

    @Override
    public ValueType getValueType() {
        return ValueType.NUMBER;
    }

    @Override
    public String toString() {
        return bigDecimal.toString();
    }
}
