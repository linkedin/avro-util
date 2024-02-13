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
public class CharSequenceMapView extends AbstractMap<CharSequence, CharSequence> {

  private Map<Utf8, Utf8> utf8Map;

  public CharSequenceMapView(Map<Utf8, Utf8> utf8Map) {
    this.utf8Map = utf8Map;
  }

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
}
