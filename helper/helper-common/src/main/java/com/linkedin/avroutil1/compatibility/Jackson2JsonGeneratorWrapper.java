/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class Jackson2JsonGeneratorWrapper implements JsonGeneratorWrapper<JsonGenerator> {

    private final JsonGenerator delegate;

    public Jackson2JsonGeneratorWrapper(JsonGenerator delegate) {
        this.delegate = delegate;
    }

    @Override
    public JsonGenerator getDelegate() {
        return delegate;
    }

    @Override
    public void writeStartObject() throws IOException {
        delegate.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        delegate.writeEndObject();
    }

    @Override
    public void writeStartArray() throws IOException {
        delegate.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        delegate.writeEndArray();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        delegate.writeFieldName(name);
    }

    @Override
    public void writeNumberField(String fieldName, long value) throws IOException {
        delegate.writeNumberField(fieldName, value);
    }

    @Override
    public void writeStringField(String fieldName, String value) throws IOException {
        delegate.writeStringField(fieldName, value);
    }

    @Override
    public void writeString(String text) throws IOException {
        delegate.writeString(text);
    }

    @Override
    public void writeArrayFieldStart(String fieldName) throws IOException {
        delegate.writeArrayFieldStart(fieldName);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }
}
