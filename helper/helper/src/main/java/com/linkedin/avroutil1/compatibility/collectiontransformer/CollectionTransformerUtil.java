/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringUtils;


public class CollectionTransformerUtil {
  private CollectionTransformerUtil() {
  }

  public static String getErrorMessageForInstance(Object obj) {
    return String.valueOf(obj) + ((obj == null) ? StringUtils.EMPTY_STRING
        : " (an instance of " + obj.getClass().getName() + ")");
  }
}
