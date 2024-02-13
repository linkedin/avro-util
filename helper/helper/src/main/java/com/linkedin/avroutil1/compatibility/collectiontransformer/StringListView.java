/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.util.AbstractList;
import org.apache.avro.util.Utf8;


/**
 * View of List<Utf8> to allow get as String while still allowing set to reflect on the original object.
 */
public class StringListView extends AbstractList<String> {
  // Not final to allow addition
  private java.util.List<Utf8> _utf8List;

  public StringListView(java.util.List<Utf8> utf8List) {
    this._utf8List = utf8List;
  }

  @Override
  public String get(int index) {
    return String.valueOf(_utf8List.get(index));
  }

  @Override
  public int size() {
    return _utf8List.size();
  }

  @Override
  public String set(int index, String element) {
    String previousValue = String.valueOf(_utf8List.get(index));
    _utf8List.set(index, new Utf8(element));
    return previousValue;
  }

  @Override
  public void add(int index, String element) {
    _utf8List.add(index, new Utf8(element));
  }

  @Override
  public boolean add(String element) {
    return _utf8List.add(new Utf8(element));
  }

  @Override
  public boolean addAll(int index, java.util.Collection<? extends String> c) {
    boolean modified = false;
    for (String element : c) {
      _utf8List.add(index++, new Utf8(element));
      modified = true;
    }
    return modified;
  }
}
