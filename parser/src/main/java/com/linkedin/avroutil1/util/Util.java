/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.linkedin.avroutil1.model.JsonPropertiesContainer;
import com.linkedin.avroutil1.model.TextLocation;
import com.linkedin.avroutil1.parser.jsonpext.JsonArrayExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonNumberExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonObjectExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonStringExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;
import javax.json.JsonValue;
import javax.json.stream.JsonLocation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Util {

    public static String read(File file) throws IOException {
        try (
                FileInputStream is = new FileInputStream(file);
                InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)
        ) {
            StringWriter writer = new StringWriter();
            char[] buffer = new char[10 * 1024];
            int chars = reader.read(buffer);
            while (chars >= 0) {
                writer.write(buffer, 0, chars);
                chars = reader.read(buffer);
            }
            return writer.toString();
        }
    }

    /**
     * converts json values to:
     * <ul>
     *     <li>{@link com.linkedin.avroutil1.model.JsonPropertiesContainer#NULL_VALUE} for json nulls</li>
     *     <li>Booleans for json boolean values</li>
     *     <li>BigDecimals for json numeric values</li>
     *     <li>java.lang.Strings for json strings</li>
     *     <li>
     *         java.util.List&lt;Object&gt;s for json arrays, where individual members
     *         are anything on this list.
     *     </li>
     *     <li>
     *         java.util.LinkedHashMap&lt;String,Object&gt;s for json objects, where
     *         keys are strings, values are anything on this list, and property order
     *         is preserved
     *     </li>
     * </ul>
     * @param jsonValue a json value
     * @return value converted according to the doc
     */
    public static Object convertJsonValue(JsonValueExt jsonValue) {
        if (jsonValue == null) {
            throw new IllegalArgumentException("jsonValue cannot be null");
        }
        JsonValue.ValueType jsonType = jsonValue.getValueType();
        switch (jsonType) {
            case NULL:
                return JsonPropertiesContainer.NULL_VALUE;
            case TRUE:
                return Boolean.TRUE;
            case FALSE:
                return Boolean.FALSE;
            case NUMBER:
                return ((JsonNumberExt) jsonValue).bigDecimalValue();
            case STRING:
                return ((JsonStringExt) jsonValue).getString();
            case ARRAY:
                JsonArrayExt jsonArray = (JsonArrayExt) jsonValue;
                List<Object> list = new ArrayList<>(jsonArray.size());
                for (JsonValue val : jsonArray) {
                    list.add(convertJsonValue((JsonValueExt) val));
                }
                return list;
            case OBJECT:
                JsonObjectExt jsonObj = (JsonObjectExt) jsonValue;
                Set<Map.Entry<String, JsonValue>> entries = jsonObj.entrySet();
                LinkedHashMap<String, Object> map = new LinkedHashMap<>(entries.size());
                for (Map.Entry<String, JsonValue> entry : entries) {
                    String key = entry.getKey();
                    JsonValueExt val = (JsonValueExt) entry.getValue();
                    map.put(key, convertJsonValue(val));
                }
                return map;
            default:
                throw new IllegalStateException("unhandled json type " + jsonType + " for "
                        + jsonValue + " at " + jsonValue.getStartLocation());
        }
    }

    public static TextLocation convertLocation (JsonLocation jsonLocation) {
        return new TextLocation(jsonLocation.getLineNumber(), jsonLocation.getColumnNumber(), jsonLocation.getStreamOffset());
    }

    public static Throwable rootCause(Throwable throwable) {
        if (throwable == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        Throwable cause = throwable;
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
