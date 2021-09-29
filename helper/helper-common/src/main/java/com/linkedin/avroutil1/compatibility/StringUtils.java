/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

public class StringUtils {

    private StringUtils() {
        //util class
    }

    /**
     * @param str a string
     * @return true if the string starts and ends with double quotes
     */
    public static boolean isQuoted(String str) {
        if (str == null) {
            return false;
        }
        return str.startsWith("\"") && str.endsWith("\"");
    }

    public static String stripQuotes(String quoted) {
        if (!isQuoted(quoted)) {
            throw new IllegalArgumentException("argument " + quoted + " is not quoted");
        }
        return quoted.substring(1, quoted.length() - 1);
    }

    public static String quote(String str) {
        return "\"" + str + "\"";
    }
}
