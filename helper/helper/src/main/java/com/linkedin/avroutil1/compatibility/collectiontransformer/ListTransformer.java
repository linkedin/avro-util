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


public class ListTransformer {
  static boolean hasCharSeq = false;

  public static List getUtf8List(Object listObj) {
    flushCharSeqFlag();
    return getUtf8(listObj);
  }

  public static List getStringList(Object listObj) {
    flushCharSeqFlag();
    return getString(listObj);
  }
  private static List getUtf8(Object listObj) {
    if(listObj == null) return null;
    if (listObj instanceof List) {
      List list = (List) listObj;
      List ret = new ArrayList(list.size());
      for (Object item : list) {
        if (item instanceof List) {
          ret.add(ListTransformer.getUtf8((List) item));
        } else if (item instanceof Map) {
          hasCharSeq = true;
          ret.add(MapTransformer.getUtf8Map((Map) item));
        } else if (item instanceof CharSequence) {
          hasCharSeq = true;
          ret.add(StringConverterUtil.getUtf8(item));
        } else {
          ret.add(item);
        }
      }
      return hasCharSeq? Collections.unmodifiableList(ret) : list;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }

  private static void flushCharSeqFlag() {
    hasCharSeq = false;
  }

  private static List getString(Object listObj) {
    if(listObj == null) return null;
    List ret;
    if(listObj instanceof List) {
      List list = (List) listObj;
      ret = new ArrayList(list.size());
      for (Object item : list) {
        if (item instanceof List) {
          ret.add(ListTransformer.getString((List) item));
        } else if (item instanceof Map) {
          hasCharSeq = true;
          ret.add(MapTransformer.getStringMap((Map) item));
        } else if (item instanceof CharSequence) {
          hasCharSeq = true;
          ret.add(StringConverterUtil.getString(item));
        } else {
          ret.add(item);
        }
      }
      return hasCharSeq? Collections.unmodifiableList(ret) : list;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }
}
