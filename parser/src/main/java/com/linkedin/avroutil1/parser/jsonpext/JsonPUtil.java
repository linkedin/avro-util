/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.JsonValue;
import javax.json.stream.JsonLocation;

public class JsonPUtil {
    private JsonPUtil() {
        //util class
    }

    /**
     * returns a more intuitive type for some json types, useful for describing errors
     * @param valType a jsonp type
     * @return type description
     */
    public static String describe(JsonValue.ValueType valType) {
        //we want to lump true and false (which are separate types) into "boolean" for purposes of clarity
        if (valType == JsonValue.ValueType.TRUE || valType == JsonValue.ValueType.FALSE) {
            return "BOOLEAN";
        }
        return valType.name();
    }

    /**
     * given an end location and a number of characters, calculates a start location.
     * this is a heuristic used to improve error messages and is not guaranteed to be accurate.
     * @param endLocation a location in a body of text where something ends
     * @param charsToSubtract the "size" of the thing that ends at endLocation
     * @return an estimated start location
     */
    public static JsonLocation subtract (JsonLocation endLocation, int charsToSubtract) {
        //this logic is naive, and will probably mess up for multi-line strings and
        //"wide-characters" (emojis, weird unicode codepoints ...). but good enough for now
        return new JsonLocationImpl(
                endLocation.getLineNumber(),
                endLocation.getColumnNumber() - charsToSubtract,
                endLocation.getStreamOffset() - charsToSubtract
        ) ;
    }
}
