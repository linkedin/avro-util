/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Collections;
import java.util.Set;

public interface JsonPropertiesContainer {
    /**
     * used to represent null json property values
     */
    Object NULL_VALUE = new Object() {
        @Override
        public String toString() {
            return "<JSON null>";
        }
    };

    JsonPropertiesContainer EMPTY = new JsonPropertiesContainer() {
        @Override
        public Set<String> propertyNames() {
            return Collections.emptySet();
        }

        @Override
        public Object getPropertyAsObject(String key) {
            return null;
        }

        @Override
        public String getPropertyAsJsonLiteral(String key) {
            return null;
        }
    };

    /**
     * @return returns all the "extra" (non core) properties defined
     * on an avro schema part, in order of definition in the source
     */
    Set<String> propertyNames();

    /**
     * returns the value of a (non-"core") property
     * types of values returned:
     * <ul>
     *     <li>NULL_VALUE for null json properties</li>
     *     <li>Booleans for boolean json properties</li>
     *     <li>BigDecimals for numeric json properties</li>
     *     <li>java.lang.Strings for string properties</li>
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
     * @param key name of the property to get
     * @return value of a type described in the doc, or null if no such property
     */
    Object getPropertyAsObject(String key);

    /**
     * returns the value of a property as a json literal:
     * <ul>
     *     <li>json null values are returned as "null" (unquoted)</li>
     *     <li>booleans are returned as "true" or "false" (unquoted)</li>
     *     <li>numbers are returned as their string representation (unquoted)</li>
     *     <li>strings are returned quoted, and escaped</li>
     *     <li>lists are returned as a json literal array (csv in square brackets)</li>
     *     <li>objects are serialized into a json document string</li>
     * </ul>
     * TODO - think about defining floats vs ints vs scientific notation for numeric values?
     * @param key name of the property to get
     * @return value as a json literal, or null if no such property
     */
    String getPropertyAsJsonLiteral(String key);
}
