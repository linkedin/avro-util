/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.IOException;

/**
 * internal abstraction to bridge jackson 1 to 2 JsonGenerator
 */
public interface JsonGeneratorWrapper<T> extends AutoCloseable {

    T getDelegate();

    void writeStartObject() throws IOException;

    void writeEndObject() throws IOException;

    void writeStartArray() throws IOException;

    void writeEndArray() throws IOException;

    void writeFieldName(String name) throws IOException;

    void writeNumberField(String fieldName, long value) throws IOException;

    void writeStringField(String fieldName, String value) throws IOException;

    void writeString(String text) throws IOException;

    void writeArrayFieldStart(String fieldName) throws IOException;

    void flush() throws IOException;

    void close() throws IOException;
}
