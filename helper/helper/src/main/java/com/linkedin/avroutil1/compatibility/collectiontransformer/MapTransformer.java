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
import org.apache.avro.util.Utf8;


public class MapTransformer {

  public static Map getUtf8Map(Object mapObj) {
    return getUtf8Map(mapObj, false);
  }

  public static Map getStringMap(Object mapObj) {
    return getStringMap(mapObj, false);
  }

  public static Map getCharSequenceMap(Object mapObj) {
    return getCharSequenceMap(mapObj, false);
  }

  public static Map getUtf8Map(Object mapObj, boolean isPrimitiveCollection) {
    if(isPrimitiveCollection) {
      return CollectionTransformerUtil.createUtf8MapView((Map<Utf8, Utf8>) mapObj);
    }
    if (mapObj == null) {
      return null;
    }
    Map ret;
    if (mapObj instanceof Map) {
      Map map = (Map) mapObj;
      ret = new HashMap(map.size());
      for (Object entry : map.entrySet()) {
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
    } else {
      throw new UnsupportedOperationException(
          "Supports only Map. Received" + CollectionTransformerUtil.getErrorMessageForInstance(mapObj));
    }

    return Collections.unmodifiableMap(ret);
  }

  public static Map getStringMap(Object mapObj, boolean isPrimitiveCollection) {
    if(isPrimitiveCollection) {
      return CollectionTransformerUtil.createStringMapView((Map<Utf8, Utf8>) mapObj);
    }
    if (mapObj == null) {
      return null;
    }
    Map ret;
    if (mapObj instanceof Map) {
      Map map = (Map) mapObj;
      ret = new HashMap(map.size());
      for (Object entry : map.entrySet()) {
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
    } else {
      throw new UnsupportedOperationException(
          "Supports only Map. Received" + CollectionTransformerUtil.getErrorMessageForInstance(mapObj));
    }
    return Collections.unmodifiableMap(ret);
  }

  public static Map getCharSequenceMap(Object mapObj, boolean isPrimitiveCollection) {
    if(isPrimitiveCollection) {
      return CollectionTransformerUtil.createCharSequenceMapView((Map<Utf8, Utf8>) mapObj);
    }
    if (mapObj == null) {
      return null;
    }
    Map ret;
    if (mapObj instanceof Map) {
      Map map = (Map) mapObj;
      ret = new HashMap(map.size());
      for (Object entry : map.entrySet()) {
        Object key = ((Map.Entry) entry).getKey();
        Object val = ((Map.Entry) entry).getValue();
        if (val instanceof List) {
          ret.put(StringConverterUtil.getString(key), ListTransformer.getCharSequenceList((List) val));
        } else if (val instanceof Map) {
          ret.put(StringConverterUtil.getString(key), MapTransformer.getCharSequenceMap((Map) val));
        } else if (val instanceof CharSequence) {
          ret.put(StringConverterUtil.getString(key), StringConverterUtil.getCharSequence(val));
        } else {
          ret.put(StringConverterUtil.getString(key), val);
        }
      }
    } else {
      throw new UnsupportedOperationException(
          "Supports only Map. Received" + CollectionTransformerUtil.getErrorMessageForInstance(mapObj));
    }
    return Collections.unmodifiableMap(ret);
  }

  public static Map convertToUtf8(Object mapObj) {
    if (mapObj == null) {
      return null;
    }
    if (mapObj instanceof Map) {
      Map map = (Map) mapObj;
      Map ret = new HashMap(map.size());
      // for all elements in the map
      for(Object entry : map.entrySet()) {
        Object key = ((Map.Entry) entry).getKey();
        Object val = ((Map.Entry) entry).getValue();
        // recursively convert to Utf8 if applicable
        if (val instanceof List) {
          ret.put(StringConverterUtil.getUtf8(key), ListTransformer.convertToUtf8((List) val));
        } else if (val instanceof Map) {
          ret.put(StringConverterUtil.getUtf8(key), MapTransformer.convertToUtf8((Map) val));
        } else if (val instanceof CharSequence) {
          ret.put(StringConverterUtil.getUtf8(key), StringConverterUtil.getUtf8(val));
        } else {
          ret.put(StringConverterUtil.getUtf8(key), val);
        }
      }
      return ret;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Map. Received" + CollectionTransformerUtil.getErrorMessageForInstance(mapObj));
    }
  }

  public static Map convertToString(Object mapObj) {
    if (mapObj == null) {
      return null;
    }
    if (mapObj instanceof Map) {
      Map map = (Map) mapObj;
      Map ret = new HashMap(map.size());
      // for all elements in the map
      for(Object entry : map.entrySet()) {
        Object key = ((Map.Entry) entry).getKey();
        Object val = ((Map.Entry) entry).getValue();
        // recursively convert to String if applicable
        if (val instanceof List) {
          ret.put(StringConverterUtil.getString(key), ListTransformer.convertToString((List) val));
        } else if (val instanceof Map) {
          ret.put(StringConverterUtil.getString(key), MapTransformer.convertToString((Map) val));
        } else if (val instanceof CharSequence) {
          ret.put(StringConverterUtil.getString(key), StringConverterUtil.getString(val));
        } else {
          ret.put(StringConverterUtil.getString(key), val);
        }
      }
      return ret;
    } else {
      throw new UnsupportedOperationException(
          "Supports only Map. Received" + CollectionTransformerUtil.getErrorMessageForInstance(mapObj));
    }
  }
}
