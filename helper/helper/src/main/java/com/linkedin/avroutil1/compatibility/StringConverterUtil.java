/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.util.Utf8;


/***
 * String util used in codegen to convert between Charsequence types (String, Utf8, Charsequence)
 */
public class StringConverterUtil {

  private StringConverterUtil() {
  }

  private static void validateArgs(Object arg) {
    if(! (arg instanceof CharSequence)) {
      throw new IllegalArgumentException("StringConverterUtil accepts only CharSequence arguments");
    }
  }

  public static String getString(Object value) {
    if (value == null) {
      return null;
    }
    validateArgs(value);
    return String.valueOf(value);
  }

  public static Utf8 getUtf8(Object value) {
    if (value == null) {
      return null;
    }
    validateArgs(value);
    return new Utf8(String.valueOf(value));
  }

  public static CharSequence getCharSequence(Object value) {
    if (value == null) {
      return null;
    }
    validateArgs(value);
    return String.valueOf(value);
  }
}
