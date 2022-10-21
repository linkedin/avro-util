/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringConverterUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ListTransformer extends ArrayList implements List {

  public static List getUtf8List(List list) {
    if(list == null) return null;
    List ret = new ArrayList(list.size());
    for (Object item : list) {
      if (item instanceof List) {
        ret.add(ListTransformer.getUtf8List((List) item));
      } else if (item instanceof Map) {
        ret.add(MapTransformer.getUtf8Map((Map) item));
      } else if (item instanceof CharSequence) {
        ret.add(StringConverterUtil.getUtf8(item));
      } else {
        ret.add(item);
      }
    }
    return Collections.unmodifiableList(ret);
  }

  public static List getStringList(List list) {
    if(list == null) return null;
    List ret = new ArrayList(list.size());
    for (Object item : list) {
      if (item instanceof List) {
        ret.add(ListTransformer.getStringList((List) item));
      } else if (item instanceof Map) {
        ret.add(MapTransformer.getStringMap((Map) item));
      } else if (item instanceof CharSequence) {
        ret.add(StringConverterUtil.getString(item));
      } else {
        ret.add(item);
      }
    }
    return Collections.unmodifiableList(ret);
  }
}
