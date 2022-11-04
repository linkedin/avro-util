/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.collectiontransformer;

import com.linkedin.avroutil1.compatibility.StringConverterUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/***
 * tests round trip conversion on :
 * List
 * List<List>
 * List<Map>
 *
 * Map
 * Map<Map>
 * Map<List>
 */
public class CollectionTransformerTest {
  List<String> strList;
  List<String> strList2;
  List<Utf8> utf8List;
  List<Utf8> utf8List2;

  Map mapStr;

  Map mapStr2;

  @Test
  public void testNull() {
    Assert.assertNull(ListTransformer.getStringList(null));
    Assert.assertNull(ListTransformer.getUtf8List(null));

    Assert.assertNull(MapTransformer.getStringMap(null));
    Assert.assertNull(MapTransformer.getUtf8Map(null));
  }

  @Test
  public void testListOfString() {

    int i = 0;
    List retList = ListTransformer.getUtf8List(strList);
    for(Object item : retList) {
      Assert.assertTrue(item instanceof Utf8);
      Assert.assertEquals(item, utf8List.get(i++));
    }

    i = 0;
    List retListStr = ListTransformer.getStringList(retList);
    for(Object item : retListStr) {
      Assert.assertEquals(item, strList.get(i++));
    }

  }

  @Test
  public void testListOfListOfString() {
    List<List<String>> listOfListOfStr = Arrays.asList(strList, strList2);
    int i = 0;
    List retList = ListTransformer.getUtf8List(listOfListOfStr);
    for(Object item : (List) retList.get(0)) {
      Assert.assertTrue(item instanceof Utf8);
      Assert.assertEquals(item, utf8List.get(i++));
    }
    i = 0;
    for(Object item : (List) retList.get(1)) {
      Assert.assertTrue(item instanceof Utf8);
      Assert.assertEquals(item, utf8List2.get(i++));
    }


    List retList2 = ListTransformer.getStringList(retList);
    i = 0;
    for(Object item : (List) retList2.get(0)) {
      Assert.assertTrue(item instanceof String);
      Assert.assertEquals(item, strList.get(i++));
    }
    i = 0;
    for(Object item : (List) retList2.get(1)) {
      Assert.assertTrue(item instanceof String);
      Assert.assertEquals(item, strList2.get(i++));
    }
  }

  @Test
  public void testMap() {
    Map retMap = MapTransformer.getUtf8Map(mapStr);
    for(Object key: retMap.keySet()) {
      Assert.assertTrue(key instanceof Utf8);
      Assert.assertTrue(retMap.get(key) instanceof Utf8);
    }

    for(String possibleKey : strList) {
      Assert.assertTrue(retMap.containsKey(new Utf8(possibleKey)));
    }

    Map retMap2 = MapTransformer.getStringMap(retMap);
    Assert.assertEquals(retMap2, mapStr);
  }

  @Test
  public void testMapOfMap() {
    Map mapOfMap = new HashMap();
    mapOfMap.put("key", mapStr);

    Map retMap = MapTransformer.getUtf8Map(mapOfMap);
    for(Object key: retMap.keySet()) {
      System.err.println(key);
      Assert.assertTrue(key instanceof Utf8);
      Assert.assertTrue(retMap.get(key) instanceof Map);
    }

    Map mapFromKey = (Map) retMap.get(new Utf8("key"));
    for(Object key : mapStr.keySet()) {
      Assert.assertEquals(mapFromKey.get(StringConverterUtil.getUtf8(key)),
          StringConverterUtil.getUtf8(mapStr.get(key)));
    }

    Map retMap2 = MapTransformer.getStringMap(retMap);
    Assert.assertEquals(retMap2.get("key"), mapStr);

  }

  @Test
  public void testListOfMap() {
    List listOfMaps = Arrays.asList(mapStr, mapStr2);
    List retList = ListTransformer.getUtf8List(listOfMaps);

    Map retMap1 = MapTransformer.getUtf8Map(mapStr);
    Map retMap2 = MapTransformer.getUtf8Map(mapStr2);

    Assert.assertEquals(retList.get(0), retMap1);
    Assert.assertEquals(retList.get(1), retMap2);

    List retList2 = ListTransformer.getStringList(retList);

    Assert.assertEquals(retList2.get(0), mapStr);
    Assert.assertEquals(retList2.get(1), mapStr2);

  }

  @Test
  public void testMapOfList() {
    Map map = new HashMap();
    map.put("list1", strList);
    map.put("list2", strList2);

    Map retMap = MapTransformer.getUtf8Map(map);

    Assert.assertEquals(retMap.get(new Utf8("list1")), ListTransformer.getUtf8List(strList));
    Assert.assertEquals(retMap.get(new Utf8("list2")), ListTransformer.getUtf8List(strList2));

    Map retMap2 = MapTransformer.getStringMap(retMap);
    Assert.assertEquals(retMap2.get("list1"), strList);
    Assert.assertEquals(retMap2.get("list2"), strList2);
  }


  @Test
  public void testUnmodifiableResponse() {
    Assert.assertThrows(UnsupportedOperationException.class, () -> ListTransformer.getUtf8List(strList).add(""));
    Assert.assertThrows(UnsupportedOperationException.class, () -> MapTransformer.getUtf8Map(mapStr).put("", ""));
  }

  @Test
  public void testObjectString() {
    List<Object> listWithCharSeq = new ArrayList<>();
    listWithCharSeq.add(new Object());
    listWithCharSeq.add(Long.valueOf(1));
    listWithCharSeq.add(null);
    listWithCharSeq.add(new Utf8("str"));


    List<Object> listWithoutCharSeq = new ArrayList<>();
    listWithoutCharSeq.add(new Object());
    listWithoutCharSeq.add(Long.valueOf(1));
    listWithoutCharSeq.add(null);

    List ret1 = ListTransformer.getUtf8List(listWithCharSeq);
    List ret2 = ListTransformer.getUtf8List(listWithoutCharSeq);


    Assert.assertNotSame(ret1, listWithCharSeq);
    Assert.assertEquals(ret1, listWithCharSeq);

    Assert.assertSame(ret2, listWithoutCharSeq);
  }

  @BeforeClass
  public void setup() {
    strList = Arrays.asList("this", "is", "a", "great", "set", "of", "words");
    strList2 = Arrays.asList("an","even", "greater", "set", "of", "combination", "of", "characters");
    utf8List = strList.stream().map(Utf8::new).collect(Collectors.toList());
    utf8List2 = strList2.stream().map(Utf8::new).collect(Collectors.toList());

    mapStr = new HashMap<>();
    for(int i = 0; i < strList.size(); i++) {
      mapStr.put(strList.get(i), strList2.get(i));
    }

    mapStr2 = new HashMap<>();
    for(int i = 0; i < strList.size(); i++) {
      mapStr2.put(strList2.get(i), strList.get(i));
    }
  }
}
