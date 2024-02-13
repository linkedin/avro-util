/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;


/**
 * View of Map<Utf8, Utf8> to allow get as String while still allowing put to reflect on the original object.
 */
public class StringMapView extends AbstractMap<String, String> {

  private Map<Utf8, Utf8> utf8Map;

  public StringMapView(Map<Utf8, Utf8> utf8Map) {
    this.utf8Map = utf8Map;
  }

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
}
