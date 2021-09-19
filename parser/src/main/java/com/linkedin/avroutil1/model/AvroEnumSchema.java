/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AvroEnumSchema extends AvroNamedSchema {
    private final List<String> symbols;
    private final String defaultSymbol;

    public AvroEnumSchema(
            CodeLocation codeLocation,
            String simpleName,
            String namespace,
            String doc,
            List<String> symbols,
            String defaultSymbol
    ) {
        super(codeLocation, simpleName, namespace, doc);
        //TODO - check for dup symbols, same-name-different-case, etc
        //TODO - check default (if exists) is a symbol
        this.symbols = Collections.unmodifiableList(new ArrayList<>(symbols));
        this.defaultSymbol = defaultSymbol;
    }

    @Override
    public AvroType type() {
        return AvroType.ENUM;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public String getDefaultSymbol() {
        return defaultSymbol;
    }
}
