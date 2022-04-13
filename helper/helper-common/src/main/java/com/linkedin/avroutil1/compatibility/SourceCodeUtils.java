/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.List;

public class SourceCodeUtils {

    private SourceCodeUtils() {
        //util class
    }

    /**
     * splits a large java string literal into smaller pieces in a safe way.
     * by safe we mean avoids splitting anywhere near an escape sequence
     * @param javaStringLiteral large string literal
     * @param maxChunkSize max chunk size in characters
     * @return smaller string literals that can be joined to reform the argument
     * TODO - change this method to calculate chunk sizes in utf-8 bytes
     */
    public static List<String> safeSplit(String javaStringLiteral, int maxChunkSize) {
        String remainder = javaStringLiteral;
        List<String> results = new ArrayList<>(remainder.length() / maxChunkSize);
        while (remainder.length() > maxChunkSize) {
            int cutIndex = maxChunkSize;
            while (cutIndex > 0 && escapesNear(remainder, cutIndex)) {
                cutIndex--;
            }
            if (cutIndex <= 0) {
                //should never happen ...
                throw new IllegalStateException("unable to split " + javaStringLiteral);
            }
            String piece = remainder.substring(0, cutIndex);
            results.add(piece);
            remainder = remainder.substring(cutIndex);
        }
        if (!remainder.isEmpty()) {
            results.add(remainder);
        }
        return results;
    }

    /**
     * returns true is there's a string escape sequence starting anywhere
     * near a given index in a given string literal. since the longest escape
     * sequences in java are ~5-6 characters (unicode escapes) a safety margin
     * of 10 characters is used.
     * @param literal string literal to look for escape sequences in
     * @param index index around (before) which to look for escapes
     * @return true if any escape sequence found
     */
    static boolean escapesNear(String literal, int index) {
        //we start at index because we dont want the char at the start of the next fragment
        //to be an "interesting" character either
        for (int i = index; i > Math.max(0, index - 6); i--) {
            char c = literal.charAt(i);
            if (c == '\\' || c == '"' || c == '\'') {
                return true;
            }
        }
        return false;
    }
}
