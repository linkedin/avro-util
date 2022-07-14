/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.commons.text.StringEscapeUtils;


public class StringUtils {

  public static final String EMPTY_STRING = "";

  private StringUtils() {
    // Util class; should not be instantiated.
  }

  /**
   * @param str a string
   * @return true if the string starts and ends with double quotes
   */
  public static boolean isQuoted(String str) {
    if (str == null) {
      return false;
    }
    return str.length() >= 2 && str.startsWith("\"") && str.endsWith("\"");
  }

  public static String unquoteAndUnescapeStringProp(String maybeAStringProp, boolean quoteStringValues,
      boolean unescapeInnerJson) {
    if (maybeAStringProp == null) {
      // no such prop
      return null;
    }
    if (quoteStringValues && !unescapeInnerJson) {
      // no changes actually required
      return maybeAStringProp;
    }
    if (!isQuoted(maybeAStringProp)) {
      // not actually a properly-quoted JSON string literal
      return maybeAStringProp;
    }
    String processed = maybeAStringProp;
    if (!quoteStringValues) {
      processed = processed.substring(1, processed.length() - 1);
    }
    if (unescapeInnerJson) {
      processed = StringEscapeUtils.unescapeJson(processed);
    }
    return processed;
  }
}
