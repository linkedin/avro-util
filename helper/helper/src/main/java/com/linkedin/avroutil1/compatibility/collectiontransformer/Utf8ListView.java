/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.util.AbstractList;
import org.apache.avro.util.Utf8;


/**
 * View of List<Utf8> to allow get as Utf8 while still allowing set to reflect on the original object.
 */
public class Utf8ListView extends AbstractList<Utf8> {
  private java.util.List<Utf8> utf8List;

  public Utf8ListView(java.util.List<Utf8> utf8List) {
    this.utf8List = utf8List;
  }

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
}
