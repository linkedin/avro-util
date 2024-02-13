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
import org.apache.avro.util.Utf8;


public class ListTransformer {
  static ThreadLocal<Boolean> hasCharSeq = new ThreadLocal<>();

  public static List getUtf8List(Object listObj) {
    flushCharSeqFlag();
    return getUtf8(listObj, false);
  }

  public static List getStringList(Object listObj) {
    flushCharSeqFlag();
    return getString(listObj, false);
  }

  public static List getCharSequenceList(Object listObj) {
    flushCharSeqFlag();
    return getCharSequence(listObj, false);
  }

  public static List getUtf8List(Object listObj, boolean isPrimitiveCollection) {
    flushCharSeqFlag();
    return getUtf8(listObj, isPrimitiveCollection);
  }

  public static List getStringList(Object listObj, boolean isPrimitiveCollection) {
    flushCharSeqFlag();
    return getString(listObj, isPrimitiveCollection);
  }

  public static List getCharSequenceList(Object listObj, boolean isPrimitiveCollection) {
    flushCharSeqFlag();
    return getCharSequence(listObj, isPrimitiveCollection);
  }

  private static List getUtf8(Object listObj, boolean isPrimitive) {
    if(listObj == null) return null;
    if(isPrimitive) return CollectionTransformerUtil.createUtf8ListView((List<Utf8>) listObj);
    if (listObj instanceof List) {
      List list = (List) listObj;
      List ret = new ArrayList(list.size());
      for (Object item : list) {
        if (item instanceof List) {
          ret.add(ListTransformer.getUtf8((List) item, false));
        } else if (item instanceof Map) {
          hasCharSeq.set(true);
          ret.add(MapTransformer.getUtf8Map((Map) item));
        } else if (item instanceof CharSequence) {
          hasCharSeq.set(true);
          ret.add(StringConverterUtil.getUtf8(item));
        } else {
          ret.add(item);
        }
      }
      return hasCharSeq.get() ? Collections.unmodifiableList(ret) : list;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }

  public static List convertToUtf8(Object listObj) {
    if (listObj == null) {
      return null;
    }
    if (listObj instanceof List) {
      List list = (List) listObj;
      List ret = new ArrayList(list.size());
      // for all items in the list
      for (Object item : list) {
        // recursively convert to Utf8 if the item is a list or a map
        if (item instanceof List) {
          ret.add(convertToUtf8((List) item));
        } else if (item instanceof Map) {
          ret.add(MapTransformer.convertToUtf8((Map) item));
          // if the item is a CharSequence, convert it to Utf8
        } else if (item instanceof CharSequence) {
          ret.add(StringConverterUtil.getUtf8(item));
        } else {
          // otherwise, add the item as is
          ret.add(item);
        }
      }
      return ret;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }

  public static List convertToString(Object listObj) {
    if (listObj == null) {
      return null;
    }
    if (listObj instanceof List) {
      List list = (List) listObj;
      List ret = new ArrayList(list.size());
      // for all items in the list
      for (Object item : list) {
        // recursively convert to String if the item is a list or a map
        if (item instanceof List) {
          ret.add(convertToString((List) item));
        } else if (item instanceof Map) {
          ret.add(MapTransformer.convertToString((Map) item));
          // if the item is a CharSequence, convert it to String
        } else if (item instanceof CharSequence) {
          ret.add(StringConverterUtil.getString(item));
        } else {
          // otherwise, add the item as is
          ret.add(item);
        }
      }
      return ret;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }

  private static void flushCharSeqFlag() {
    hasCharSeq.set(false);
  }

  private static List getString(Object listObj, boolean isPrimitive) {
    if(listObj == null) return null;
    if(isPrimitive) return CollectionTransformerUtil.createStringListView((List<Utf8>) listObj);
    List ret;
    if(listObj instanceof List) {
      List list = (List) listObj;
      ret = new ArrayList(list.size());
      for (Object item : list) {
        if (item instanceof List) {
          ret.add(ListTransformer.getString((List) item, false));
        } else if (item instanceof Map) {
          hasCharSeq.set(true);
          ret.add(MapTransformer.getStringMap((Map) item));
        } else if (item instanceof CharSequence) {
          hasCharSeq.set(true);
          ret.add(StringConverterUtil.getString(item));
        } else {
          ret.add(item);
        }
      }
      return hasCharSeq.get() ? Collections.unmodifiableList(ret) : list;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }

  private static List getCharSequence(Object listObj, boolean isPrimitive) {
    if(listObj == null) return null;
    if(isPrimitive) return CollectionTransformerUtil.createCharSequenceListView((List<Utf8>) listObj);
    List ret;
    if(listObj instanceof List) {
      List list = (List) listObj;
      ret = new ArrayList(list.size());
      for (Object item : list) {
        if (item instanceof List) {
          ret.add(ListTransformer.getString((List) item, false));
        } else if (item instanceof Map) {
          hasCharSeq.set(true);
          ret.add(MapTransformer.getStringMap((Map) item));
        } else if (item instanceof CharSequence) {
          hasCharSeq.set(true);
          ret.add(StringConverterUtil.getCharSequence(item));
        } else {
          ret.add(item);
        }
      }
      return hasCharSeq.get() ? Collections.unmodifiableList(ret) : list;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Lists. Received" + CollectionTransformerUtil.getErrorMessageForInstance(listObj));
    }
  }
}
