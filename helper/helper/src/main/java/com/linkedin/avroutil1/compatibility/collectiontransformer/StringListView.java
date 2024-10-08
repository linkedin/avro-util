/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;


/**
 * View of Utf8 List to allow get as String while still allowing set to reflect on the original object.
 */
public class StringListView extends AbstractList<String> implements Serializable {
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

  public String set(int index, Utf8 element) {
    String previousValue = String.valueOf(_utf8List.get(index));
    _utf8List.set(index, element);
    return previousValue;
  }

  @Override
  public void add(int index, String element) {
    _utf8List.add(index, new Utf8(element));
  }

  public void add(int index, Utf8 element) {
    _utf8List.add(index, element);
  }

  @Override
  public boolean add(String element) {
    return _utf8List.add(new Utf8(element));
  }

  public boolean add(Utf8 element) {
    return _utf8List.add(element);
  }

  @Override
  public boolean addAll(int index, java.util.Collection<? extends String> c) {
    boolean modified = false;
    for (Object element : c) {
      if (element instanceof Utf8) {
        _utf8List.add(index++, (Utf8) element);
      } else {
        _utf8List.add(index++, new Utf8(String.valueOf(element)));
      }
      modified = true;
    }
    return modified;
  }

  @Override
  public boolean remove(Object o) {
    return _utf8List.remove(new Utf8(o.toString()));
  }

  @Override
  public void clear() {
    _utf8List.clear();
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private final Iterator<Utf8> _iter = _utf8List.iterator();

      @Override
      public boolean hasNext() {
        return _iter.hasNext();
      }

      @Override
      public String next() {
        return String.valueOf(_iter.next());
      }
    };
  }

  /**
   * Custom serialization to write the list of strings instead of the list of Utf8.
   * ObjectOutputStream seeks this method to serialize the object.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    out.writeObject(_utf8List.stream().map(Utf8::toString).collect(Collectors.toList()));
  }
}
