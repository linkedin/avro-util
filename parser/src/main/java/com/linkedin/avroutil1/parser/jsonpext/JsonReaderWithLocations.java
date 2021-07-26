/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.stream.JsonParser;

import java.io.Reader;
import java.nio.file.Path;

public class JsonReaderWithLocations implements JsonReaderExt {
    private final JsonParser parser;
    private final Path source;

    private boolean readDone;
    private JsonParser.Event currentEvent;

    public JsonReaderWithLocations(Reader reader, Path source) {
        this.parser = Json.createParser(reader);
        this.source = source;
        this.readDone = false;
    }

    @Override
    public JsonStructureExt read() {
        throw new UnsupportedOperationException("you want this? you write it!");
    }

    @Override
    public JsonObjectExt readObject() {
        ensureSingleUse();
        advance();
        if (!JsonParser.Event.START_OBJECT.equals(currentEvent)) {
            throw new IllegalStateException("was expecting START_OBJECT (\"{\") at location " + parser.getLocation() + " but instead found " + currentEvent);
        }
        return readObjectInternal();
    }

    @Override
    public JsonArrayExt readArray() {
        ensureSingleUse();
        throw new UnsupportedOperationException("you want this? you write it!");
    }

    @Override
    public void close() {
        readDone = true;
        parser.close();
    }

    @Override
    public JsonValueExt readValue() {
        ensureSingleUse();
        advance();
        return readJsonValueInternal();
    }

    protected JsonObjectExt readObjectInternal() {
        JsonObjectExtBuilder builder = new JsonObjectExtBuilder();
        builder.setSource(source);
        builder.setStartLocation(parser.getLocation());
        while(parser.hasNext()) {
            advance();
            if (currentEvent == JsonParser.Event.END_OBJECT) {
                // } - close object
                builder.setEndLocation(parser.getLocation());
                return builder.build();
            }
            // expecting key = value
            if (currentEvent != JsonParser.Event.KEY_NAME) {
                throw new JsonException("expecting key name at " + parser.getLocation() + " but got " + currentEvent);
            }
            String key = parser.getString();
            advance();
            JsonValueExt value = readJsonValueInternal();
            builder.add(key, value);
        }
        throw new JsonException("object that started at " + builder.getStartLocation() + " never closed");
    }

    protected JsonValueExt readJsonValueInternal() {
        switch (currentEvent) {
            case START_ARRAY:
                return readJsonArrayInternal();
            case START_OBJECT:
                return readObjectInternal();
            case KEY_NAME:
                throw new JsonException("unexpected " + currentEvent + " at " + parser.getLocation());
            //TODO - better start/end for all "short" values
            case VALUE_STRING:
                return new JsonStringExtImpl(source, parser.getLocation(), parser.getLocation(), parser.getString());
            case VALUE_NUMBER:
                //we dont bother optimizing for ints/longs
                return new JsonNumberExtImpl(source, parser.getLocation(), parser.getLocation(), parser.getBigDecimal());
            case VALUE_TRUE:
                return new JsonTrueExtImpl(source, parser.getLocation(), parser.getLocation());
            case VALUE_FALSE:
                return new JsonFalseExtImpl(source, parser.getLocation(), parser.getLocation());
            case VALUE_NULL:
                return new JsonNullExtImpl(source, parser.getLocation(), parser.getLocation());
            case END_ARRAY:
                throw new JsonException("unexpected " + currentEvent + " at " + parser.getLocation());
            case END_OBJECT:
                throw new JsonException("unexpected " + currentEvent + " at " + parser.getLocation());
            default:
                throw new JsonException("unexpected " + currentEvent + " at " + parser.getLocation());
        }
    }

    protected JsonArrayExt readJsonArrayInternal() {
        JsonArrayExtBuilder builder = new JsonArrayExtBuilder();
        builder.setSource(source);
        builder.setStartLocation(parser.getLocation());
        while(parser.hasNext()) {
            advance();
            if (currentEvent == JsonParser.Event.END_ARRAY) {
                // ] - close array
                builder.setEndLocation(parser.getLocation());
                return builder.build();
            }
            // expecting value
            JsonValueExt value = readJsonValueInternal();
            builder.add(value);
        }
        throw new JsonException("array that started at " + builder.getStartLocation() + " never closed");
    }

    /**
     * ensures this class can only be used once to read a single top-level json value
     * as per the docs on {@link jakarta.json.JsonReader}
     */
    protected void ensureSingleUse() {
        if (readDone) {
            throw new IllegalStateException("reader is done");
        }
        readDone = true;
        if (!parser.hasNext()) {
            throw new IllegalStateException("parser has no next element?!");
        }
    }

    protected void advance() {
        currentEvent = parser.next();
    }
}
