/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringUtils;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;


public class CollectionTransformerUtil {
  private CollectionTransformerUtil() {
  }

  public static String getErrorMessageForInstance(Object obj) {
    return String.valueOf(obj) + ((obj == null) ? StringUtils.EMPTY_STRING
        : " (an instance of " + obj.getClass().getName() + ")");
  }

  public static List<String> createStringListView(List<Utf8> utf8List) {
    return new AbstractList<String>() {
      @Override
      public String get(int index) {
        return String.valueOf(utf8List.get(index));
      }

      @Override
      public int size() {
        return utf8List.size();
      }

      @Override
      public String set(int index, String element) {
        String previousValue = String.valueOf(utf8List.get(index));
        utf8List.set(index, new Utf8(element));
        return previousValue;
      }

      @Override
      public void add(int index, String element) {
        utf8List.add(index, new Utf8(element));
      }

      @Override
      public boolean add(String element) {
        return utf8List.add(new Utf8(element));
      }

      @Override
      public boolean addAll(int index, java.util.Collection<? extends String> c) {
        boolean modified = false;
        for (String element : c) {
          utf8List.add(index++, new Utf8(element));
          modified = true;
        }
        return modified;
      }
    };
  }

  public static List<CharSequence> createCharSequenceListView(List<Utf8> utf8List) {
    return new AbstractList<CharSequence>() {
      @Override
      public CharSequence get(int index) {
        return String.valueOf(utf8List.get(index));
      }

      @Override
      public int size() {
        return utf8List.size();
      }

      @Override
      public CharSequence set(int index, CharSequence element) {
        CharSequence previousValue = String.valueOf(utf8List.get(index));
        utf8List.set(index, new Utf8(element.toString()));
        return previousValue;
      }

      @Override
      public void add(int index, CharSequence element) {
        utf8List.add(index, new Utf8(element.toString()));
      }

      @Override
      public boolean add(CharSequence element) {
        return utf8List.add(new Utf8(element.toString()));
      }

      @Override
      public boolean addAll(int index, java.util.Collection<? extends CharSequence> c) {
        boolean modified = false;
        for (CharSequence element : c) {
          utf8List.add(index++, new Utf8(element.toString()));
          modified = true;
        }
        return modified;
      }

    };
  }

  public static List<Utf8> createUtf8ListView(List<Utf8> utf8List) {
    return new AbstractList<Utf8>() {
      @Override
      public Utf8 get(int index) {
        return utf8List.get(index);
      }

      @Override
      public int size() {
        return utf8List.size();
      }

      @Override
      public Utf8 set(int index, Utf8 element) {
        Utf8 previousValue = utf8List.get(index);
        utf8List.set(index, element);
        return previousValue;
      }

      @Override
      public void add(int index, Utf8 element) {
        utf8List.add(index, element);
      }

      @Override
      public boolean add(Utf8 element) {
        return utf8List.add(element);
      }

      @Override
      public boolean addAll(int index, java.util.Collection<? extends Utf8> c) {
        return utf8List.addAll(index, c);
      }
    };
  }


  public static Map<String, String> createStringMapView(Map<Utf8, Utf8> utf8Map) {
    if (utf8Map == null) {
      return null;
    }

    return new AbstractMap<String, String>() {
      @Override
      public Set<Entry<String, String>> entrySet() {
        return Collections.unmodifiableSet(utf8Map.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> String.valueOf(entry.getKey()),
                entry -> String.valueOf(entry.getValue())
            ))
            .entrySet());
      }

      @Override
      public String put(String key, String value) {
        Utf8 utf8Key = new Utf8(key);
        Utf8 utf8Value = new Utf8(value);
        Utf8 previousValue = utf8Map.put(utf8Key, utf8Value);
        return previousValue != null ? String.valueOf(previousValue) : null;
      }

      @Override
      public Set<String> keySet() {
        return Collections.unmodifiableSet(utf8Map.keySet().stream()
            .map(String::valueOf)
            .collect(Collectors.toSet()));
      }

      @Override
      public Collection<String> values() {
        return Collections.unmodifiableCollection(utf8Map.values().stream()
            .map(String::valueOf)
            .collect(Collectors.toList()));
      }

      @Override
      public int size() {
        return utf8Map.size();
      }

      @Override
      public boolean isEmpty() {
        return utf8Map.isEmpty();
      }

      @Override
      public boolean containsKey(Object key) {
        return utf8Map.containsKey(new Utf8(String.valueOf(key)));
      }

      @Override
      public boolean containsValue(Object value) {
        return utf8Map.containsValue(new Utf8(String.valueOf(value)));
      }

      @Override
      public String get(Object key) {
        Utf8 utf8Key = new Utf8(String.valueOf(key));
        Utf8 utf8Value = utf8Map.get(utf8Key);
        return utf8Value != null ? String.valueOf(utf8Value) : null;
      }

      @Override
      public String remove(Object key) {
        Utf8 utf8Key = new Utf8(String.valueOf(key));
        Utf8 previousValue = utf8Map.remove(utf8Key);
        return previousValue != null ? String.valueOf(previousValue) : null;
      }

      @Override
      public void putAll(Map<? extends String, ? extends String> m) {
        m.forEach((key, value) -> {
          Utf8 utf8Key = new Utf8(key);
          Utf8 utf8Value = new Utf8(value);
          utf8Map.put(utf8Key, utf8Value);
        });
      }
    };
  }

  public static Map<Utf8, Utf8> createUtf8MapView(Map<Utf8, Utf8> utf8Map) {
    return utf8Map;
  }

  public static Map<CharSequence, CharSequence> createCharSequenceMapView(Map<Utf8, Utf8> utf8Map) {
    if (utf8Map == null) {
      return null;
    }

    return new AbstractMap<CharSequence, CharSequence>() {
      @Override
      public Set<Entry<CharSequence, CharSequence>> entrySet() {
        return Collections.unmodifiableSet(utf8Map.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> (CharSequence) String.valueOf(entry.getKey()),
                entry -> (CharSequence) String.valueOf(entry.getValue())
            ))
            .entrySet());
      }

      @Override
      public CharSequence put(CharSequence key, CharSequence value) {
        Utf8 utf8Key = new Utf8(key.toString());
        Utf8 utf8Value = new Utf8(value.toString());
        Utf8 previousValue = utf8Map.put(utf8Key, utf8Value);
        return previousValue != null ? (CharSequence) String.valueOf(previousValue) : null;
      }

      @Override
      public Set<CharSequence> keySet() {
        return Collections.unmodifiableSet(utf8Map.keySet().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toSet()));
      }

      @Override
      public Collection<CharSequence> values() {
        return Collections.unmodifiableCollection(utf8Map.values().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList()));
      }

      @Override
      public int size() {
        return utf8Map.size();
      }

      @Override
      public boolean isEmpty() {
        return utf8Map.isEmpty();
      }

      @Override
      public boolean containsKey(Object key) {
        return utf8Map.containsKey(new Utf8(String.valueOf(key)));
      }

      @Override
      public boolean containsValue(Object value) {
        return utf8Map.containsValue(new Utf8(String.valueOf(value)));
      }

      @Override
      public CharSequence get(Object key) {
        Utf8 utf8Key = new Utf8(String.valueOf(key));
        Utf8 utf8Value = utf8Map.get(utf8Key);
        return utf8Value != null ? (CharSequence) String.valueOf(utf8Value) : null;
      }

      @Override
      public CharSequence remove(Object key) {
        Utf8 utf8Key = new Utf8(String.valueOf(key));
        Utf8 previousValue = utf8Map.remove(utf8Key);
        return previousValue != null ? (CharSequence) String.valueOf(previousValue) : null;
      }

      @Override
      public void putAll(Map<? extends CharSequence, ? extends CharSequence> m) {
        m.forEach((key, value) -> {
          Utf8 utf8Key = new Utf8(String.valueOf(key));
          Utf8 utf8Value = new Utf8(String.valueOf(value));
          utf8Map.put(utf8Key, utf8Value);
        });
      }
    };
  }
}
