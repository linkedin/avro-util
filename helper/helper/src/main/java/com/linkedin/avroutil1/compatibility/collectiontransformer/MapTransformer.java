/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringConverterUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MapTransformer {

  public static Map getUtf8Map(Map map) {
    if(map == null) return null;
    Map ret = new HashMap(map.size());
    for(Object entry : map.entrySet()) {
      Object key = ((Map.Entry) entry).getKey();
      Object val = ((Map.Entry) entry).getValue();
      if (val instanceof List) {
        ret.put(StringConverterUtil.getUtf8(key), ListTransformer.getUtf8List((List) val));
      } else if (val instanceof Map) {
        ret.put(StringConverterUtil.getUtf8(key), MapTransformer.getUtf8Map((Map) val));
      } else if (val instanceof CharSequence) {
        ret.put(StringConverterUtil.getUtf8(key), StringConverterUtil.getUtf8(val));
      } else {
        ret.put(StringConverterUtil.getUtf8(key), val);
      }
    }
    return Collections.unmodifiableMap(ret);
  }

  public static Map getStringMap(Map map) {
    if(map == null) return null;
    Map ret = new HashMap(map.size());
    for(Object entry : map.entrySet()) {
      Object key = ((Map.Entry) entry).getKey();
      Object val = ((Map.Entry) entry).getValue();
      if (val instanceof List) {
        ret.put(StringConverterUtil.getString(key), ListTransformer.getStringList((List) val));
      } else if (val instanceof Map) {
        ret.put(StringConverterUtil.getString(key), MapTransformer.getStringMap((Map) val));
      } else if (val instanceof CharSequence) {
        ret.put(StringConverterUtil.getString(key), StringConverterUtil.getString(val));
      } else {
        ret.put(StringConverterUtil.getString(key), val);
      }
    }
    return Collections.unmodifiableMap(ret);
  }
}
