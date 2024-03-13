/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringUtils;
import java.util.List;
import org.apache.avro.util.Utf8;


public class CollectionTransformerUtil {
  private CollectionTransformerUtil() {
  }

  public static String getErrorMessageForInstance(Object obj) {
    return String.valueOf(obj) + ((obj == null) ? StringUtils.EMPTY_STRING
        : " (an instance of " + obj.getClass().getName() + ")");
  }

  /**
   * Returns a {@link StringListView} for the given list of {@link Utf8} objects.
   * @param utf8List list of {@link Utf8} objects
   * @return a {@link StringListView} for the given list of {@link Utf8} objects
   */
  public static List<String> createStringListView(List<Utf8> utf8List) {
    return new StringListView(utf8List);
  }

  /**
   * Returns a {@link CharSequenceListView} for the given list of {@link Utf8} objects.
   * @param utf8List list of {@link Utf8} objects
   * @return a {@link CharSequenceListView} for the given list of {@link Utf8} objects
   */
  public static List<CharSequence> createCharSequenceListView(List<Utf8> utf8List) {
    return new CharSequenceListView(utf8List);
  }

  /**
   * Returns a {@link Utf8ListView} for the given list of {@link Utf8} objects.
   * @param utf8List list of {@link Utf8} objects
   * @return a {@link Utf8ListView} for the given list of {@link Utf8} objects
   */
  public static List<Utf8> createUtf8ListView(List<Utf8> utf8List) {
    return new Utf8ListView(utf8List);
  }
}
