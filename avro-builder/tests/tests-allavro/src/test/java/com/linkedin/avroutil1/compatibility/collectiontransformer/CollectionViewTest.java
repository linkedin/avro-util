/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.avroutil1.compatibility.collectiontransformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CollectionViewTest {
  @Test
  public void testStringListView() {
    List<Utf8> utf8List = new ArrayList<>();
    String element = "test";
    List<String> listOfElements = Arrays.asList("test", "test2", "test3");
    List<String> view = CollectionTransformerUtil.createStringListView(utf8List);
    // utf8 list is empty
    Assert.assertEquals(utf8List.size(), 0);
    // view should be empty
    Assert.assertEquals(view.size(), 0);

    // add a string to the view
    view.add(element);
    // view should have 1 element
    Assert.assertTrue(view.contains(element));
    // utf8 list should contain the same element
    Assert.assertTrue(utf8List.contains(new Utf8(element)));

    // remove the element from the view
    view.remove(element);
    // view should be empty
    Assert.assertEquals(view.size(), 0);
    // utf8 list should be empty
    Assert.assertEquals(utf8List.size(), 0);

    // add a list of elements to the view
    view.addAll(listOfElements);
    // view should have 3 elements
    for (String s : listOfElements) {
      Assert.assertTrue(view.contains(s));
    }
    // utf8 list should contain the same 3 elements
    for (String s : listOfElements) {
      Assert.assertTrue(utf8List.contains(new Utf8(s)));
    }
  }

  @Test
  public void testUtf8ListView() {
    List<Utf8> utf8List = new ArrayList<>();
    Utf8 element = new Utf8("test");
    List<Utf8> listOfElements = Arrays.asList(new Utf8("test"), new Utf8("test2"), new Utf8("test3"));
    List<Utf8> view = CollectionTransformerUtil.createUtf8ListView(utf8List);
    // utf8 list is empty
    Assert.assertEquals(utf8List.size(), 0);
    // view should be empty
    Assert.assertEquals(view.size(), 0);

    // add a utf8 to the view
    view.add(element);
    // view should have 1 element
    Assert.assertTrue(view.contains(element));
    // utf8 list should contain the same element
    Assert.assertTrue(utf8List.contains(element));

    // remove the element from the view
    view.remove(element);
    // view should be empty
    Assert.assertEquals(view.size(), 0);
    // utf8 list should be empty
    Assert.assertEquals(utf8List.size(), 0);

    // add a list of elements to the view
    view.addAll(listOfElements);
    // view should have 3 elements
    for (Utf8 u : listOfElements) {
      Assert.assertTrue(view.contains(u));
    }
    // utf8 list should contain the same 3 elements
    for (Utf8 u : listOfElements) {
      Assert.assertTrue(utf8List.contains(u));
    }
  }

  @Test
  public void testCharSequenceListView() {
    List<Utf8> utf8List = new ArrayList<>();
    String element = "test";
    List<Utf8> listOfElements = Arrays.asList(new Utf8("test"), new Utf8("test2"), new Utf8("test3"));
    List<CharSequence> view = CollectionTransformerUtil.createCharSequenceListView(utf8List);
    // utf8 list is empty
    Assert.assertEquals(utf8List.size(), 0);
    // view should be empty
    Assert.assertEquals(view.size(), 0);

    // add to the view
    view.add(element);
    // view should have 1 element
    Assert.assertTrue(view.contains(element));
    // utf8 list should contain the same element in Utf8 form
    Assert.assertTrue(utf8List.contains(new Utf8(element)));

    // remove the element from the view
    view.remove(element);
    // view should be empty
    Assert.assertEquals(view.size(), 0);
    // utf8 list should be empty
    Assert.assertEquals(utf8List.size(), 0);

    // add a list of elements to the view
    view.addAll(listOfElements);
    // view should have 3 elements
    for (Utf8 u : listOfElements) {
      Assert.assertTrue(view.contains(String.valueOf(u)));
    }
    // utf8 list should contain the same 3 elements
    for (Utf8 u : listOfElements) {
      Assert.assertTrue(utf8List.contains(u));
    }
  }

  @Test
  public void testStringMapView() {
    Map<Utf8, Utf8> utf8Map = new HashMap<>();
    List<String> keys = Arrays.asList("key1", "key2", "key3");
    String val = "value";

    Map<String, String> map = CollectionTransformerUtil.createStringMapView(utf8Map);

    // utf8 map is empty
    Assert.assertEquals(utf8Map.size(), 0);
    // view should be empty
    Assert.assertEquals(map.size(), 0);

    // insert in view
    for (String key : keys) {
      map.put(key, key + val);
    }

    // view should have 3 elements
    for (String key : keys) {
      Assert.assertTrue(map.containsKey(key));
    }
    // utf8 map should contain the same 3 elements
    for (String key : keys) {
      Assert.assertTrue(utf8Map.containsKey(new Utf8(key)));
    }

    // remove from map
    for (String key : keys) {
      map.remove(key);
    }
    // view should be empty
    Assert.assertEquals(map.size(), 0);
  }

  @Test
  public void testUtf8MapView() {
    Map<Utf8, Utf8> utf8Map = new HashMap<>();
    List<Utf8> keys = Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key3"));
    Utf8 val = new Utf8("value");

    Map<Utf8, Utf8> map = CollectionTransformerUtil.createUtf8MapView(utf8Map);
    // utf8 map is empty
    Assert.assertEquals(utf8Map.size(), 0);
    // view should be empty
    Assert.assertEquals(map.size(), 0);

    // insert in view
    for (Utf8 key : keys) {
      map.put(key, val);
    }

    // view should have 3 elements
    for (Utf8 key : keys) {
      Assert.assertTrue(map.containsKey(key));
    }
    // utf8 map should contain the same 3 elements
    for (Utf8 key : keys) {
      Assert.assertTrue(utf8Map.containsKey(key));
    }

    // remove from view
    for (Utf8 key : keys) {
      map.remove(key);
    }
    // view should be empty
    Assert.assertEquals(map.size(), 0);
  }

  @Test
  public void testCharSequenceMapView() {
    Map<Utf8, Utf8> utf8Map = new HashMap<>();
    List<CharSequence> keys = Arrays.asList("key1", "key2", "key3");
    String val = "value";

    Map<CharSequence, CharSequence> map = CollectionTransformerUtil.createCharSequenceMapView(utf8Map);

    // utf8 map is empty
    Assert.assertEquals(utf8Map.size(), 0);
    // view should be empty
    Assert.assertEquals(map.size(), 0);

    // insert in view
    for (CharSequence key : keys) {
      map.put(key, key + val);
    }

    // view should have 3 elements
    for (CharSequence key : keys) {
      Assert.assertTrue(map.containsKey(key));
    }
    // utf8 map should contain the same 3 elements
    for (CharSequence key : keys) {
      Assert.assertTrue(utf8Map.containsKey(new Utf8(String.valueOf(key))));
    }

    // remove from view
    for (CharSequence key : keys) {
      map.remove(key);
    }
    // view should be empty
    Assert.assertEquals(map.size(), 0);
  }
}
