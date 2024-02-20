/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.util.AbstractList;
import org.apache.avro.util.Utf8;


/**
 * View of Utf8 List to allow get as CharSequence while still allowing put to reflect on the original object.
 */
public class CharSequenceListView extends AbstractList<CharSequence> {
  private java.util.List<Utf8> utf8List;

  public CharSequenceListView(java.util.List<Utf8> utf8List) {
    this.utf8List = utf8List;
  }

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

  @Override
  public boolean remove(Object o) {
    return utf8List.remove(new Utf8(o.toString()));
  }
}
