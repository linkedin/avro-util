/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.testcommon;

public class JsonLiterals {
  // @formatter:off
  public static final String[] STRING_LITERALS = new String[]{
      "\"\"",
      "\"misha, sasha\"",
      "\"3.141592653589\"",
      "\"escaping: backslash (\\\\), slash (\\/), control chars (\\n, \\t), hex codes (\\uCAFE)\"",
      "\"true\"",
      "\"{}\"",
  };
  // A parallel array to STRING_LITERALS, containing the corresponding raw values (unquoted, unescaped).
  public static final String[] STRING_VALUES = new String[]{
      "",
      "misha, sasha",
      "3.141592653589",
      "escaping: backslash (\\), slash (/), control chars (\n, \t), hex codes (\uCAFE)",
      "true",
      "{}",
  };
  public static final String[] NON_STRING_LITERALS = new String[]{
      "true",
      "false",
      "123",
      "-4.5e6",
      "[]",
      "[7,8,9]",
      "{}",
      "{\"foo\":\"bar\",\"what is it good for?\":\"absolutely nothing\"}",
      "[[[{\"\":[[]]}]]]",
      " [ 1, 2, 3 ] ",
  };
  // A parallel array to NON_STRING_LITERALS, containing the values as serialized by Jackson.
  public static final String[] NON_STRING_VALUES = new String[]{
      "true",
      "false",
      "123",
      "-4500000.0",
      "[]",
      "[7,8,9]",
      "{}",
      "{\"foo\":\"bar\",\"what is it good for?\":\"absolutely nothing\"}",
      "[[[{\"\":[[]]}]]]",
      "[1,2,3]",
  };
  public static final String[] INVALID_LITERALS = new String[]{
      "",
      "dogs",
      "misha and sasha",
      "special chars: \"\\/\b\f\n\r\t~`!@#$%^&*({[}]|:;'<,.?)}]>\\\"\"\"\"",
      "1,2",
      "1 2",
      "1\n234567890",
      "[",
      "}",
      "foo\"\"\"\"\"\"bar",
      ",,,,",
      "\"",
      "\"qux\\\"",
      "\"lara\" + \"yuri\"",
      "\"golden\"retriever\"",
  };
  // @formatter:on

  private JsonLiterals() {
    // Util class; should not be instantiated.
  }

  static {
    assert STRING_LITERALS.length == STRING_VALUES.length;
    assert NON_STRING_LITERALS.length == NON_STRING_VALUES.length;
  }
}
