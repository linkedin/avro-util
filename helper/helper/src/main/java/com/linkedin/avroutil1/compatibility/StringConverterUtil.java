/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.nio.charset.StandardCharsets;
import org.apache.avro.util.Utf8;


public class StringConverterUtil {
  private String value;

  public StringConverterUtil(Object value) {
    if ((value instanceof CharSequence)) {
      this.value = String.valueOf(value);
    } else {
      throw new UnsupportedOperationException("Unsupported argument type.");
    }
  }

  public String getString() {
    return value;
  }

  public Utf8 getUtf8() {
    return new Utf8(value.getBytes(StandardCharsets.UTF_8));
  }

  public CharSequence getCharSequence() {
    return value;
  }
}
