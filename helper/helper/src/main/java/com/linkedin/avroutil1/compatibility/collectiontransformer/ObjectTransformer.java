/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringConverterUtil;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.util.List;
import java.util.Map;


public class ObjectTransformer {

  public static Object transform(Object input, StringRepresentation desired) {
    if (desired == null) {
      throw new IllegalArgumentException("desired argument required");
    }
    if (input == null) {
      return null;
    }
    if (input instanceof CharSequence) {
      switch (desired) {
        case Utf8:
          return StringConverterUtil.getUtf8(input);
        case String:
          return StringConverterUtil.getString(input);
        case CharSequence:
          return StringConverterUtil.getCharSequence(input);
        default:
          throw new IllegalStateException("unhandled: " + desired);
      }
    }
    if (input instanceof List) {
      switch (desired) {
        case Utf8:
          return ListTransformer.getUtf8List(input);
        case String:
          return ListTransformer.getStringList(input);
        case CharSequence:
          return ListTransformer.getCharSequenceList(input);
        default:
          throw new IllegalStateException("unhandled: " + desired);
      }
    }
    if (input instanceof Map) {
      switch (desired) {
        case Utf8:
          return MapTransformer.getUtf8Map(input);
        case String:
          return MapTransformer.getStringMap(input);
        case CharSequence:
          return MapTransformer.getCharSequenceMap(input);
        default:
          throw new IllegalStateException("unhandled: " + desired);
      }
    }
    throw new IllegalStateException("argument is not a CharSequence, List or Map, but a "
        + input.getClass().getName());
  }
}
