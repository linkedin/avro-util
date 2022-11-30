/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SpecificRecordTest {

  @DataProvider
  private Object[][] TestRoundTripSerializationProvider() {
    return new Object[][]{
        {vs14.SimpleRecord.class, vs14.SimpleRecord.getClassSchema()},
        {vs15.SimpleRecord.class, vs15.SimpleRecord.getClassSchema()},
        {vs16.SimpleRecord.class, vs16.SimpleRecord.getClassSchema()},
        {vs17.SimpleRecord.class, vs17.SimpleRecord.getClassSchema()},
        {vs18.SimpleRecord.class, vs18.SimpleRecord.getClassSchema()},
        {vs19.SimpleRecord.class, vs19.SimpleRecord.getClassSchema()},
        {vs110.SimpleRecord.class, vs110.SimpleRecord.getClassSchema()},
        {vs111.SimpleRecord.class, vs111.SimpleRecord.getClassSchema()},

        {vs14.MoneyRange.class, vs14.MoneyRange.getClassSchema()},
        {vs15.MoneyRange.class, vs15.MoneyRange.getClassSchema()},
        {vs16.MoneyRange.class, vs16.MoneyRange.getClassSchema()},
        {vs17.MoneyRange.class, vs17.MoneyRange.getClassSchema()},
        {vs18.MoneyRange.class, vs18.MoneyRange.getClassSchema()},
        {vs19.MoneyRange.class, vs19.MoneyRange.getClassSchema()},
        {vs110.MoneyRange.class, vs110.MoneyRange.getClassSchema()},
        {vs111.MoneyRange.class, vs111.MoneyRange.getClassSchema()},

        {vs14.DollarSignInDoc.class, vs14.DollarSignInDoc.getClassSchema()},
        {vs15.DollarSignInDoc.class, vs15.DollarSignInDoc.getClassSchema()},
        {vs16.DollarSignInDoc.class, vs16.DollarSignInDoc.getClassSchema()},
        {vs17.DollarSignInDoc.class, vs17.DollarSignInDoc.getClassSchema()},
        {vs18.DollarSignInDoc.class, vs18.DollarSignInDoc.getClassSchema()},
        {vs19.DollarSignInDoc.class, vs19.DollarSignInDoc.getClassSchema()},
        {vs110.DollarSignInDoc.class, vs110.DollarSignInDoc.getClassSchema()},
        {vs111.DollarSignInDoc.class, vs111.DollarSignInDoc.getClassSchema()},

        {vs14.RecordDefault.class, vs14.RecordDefault.getClassSchema()},
        {vs15.RecordDefault.class, vs15.RecordDefault.getClassSchema()},
        {vs16.RecordDefault.class, vs16.RecordDefault.getClassSchema()},
        {vs17.RecordDefault.class, vs17.RecordDefault.getClassSchema()},
        {vs18.RecordDefault.class, vs18.RecordDefault.getClassSchema()},
        {vs19.RecordDefault.class, vs19.RecordDefault.getClassSchema()},
        {vs110.RecordDefault.class, vs110.RecordDefault.getClassSchema()},
        {vs111.RecordDefault.class, vs111.RecordDefault.getClassSchema()},

        {vs14.ArrayOfRecords.class, vs14.ArrayOfRecords.getClassSchema()},
        {vs15.ArrayOfRecords.class, vs15.ArrayOfRecords.getClassSchema()},
        {vs16.ArrayOfRecords.class, vs16.ArrayOfRecords.getClassSchema()},
        {vs17.ArrayOfRecords.class, vs17.ArrayOfRecords.getClassSchema()},
        {vs18.ArrayOfRecords.class, vs18.ArrayOfRecords.getClassSchema()},
        {vs19.ArrayOfRecords.class, vs19.ArrayOfRecords.getClassSchema()},
        {vs110.ArrayOfRecords.class, vs110.ArrayOfRecords.getClassSchema()},
        {vs111.ArrayOfRecords.class, vs111.ArrayOfRecords.getClassSchema()},

        {vs14.ArrayOfStringRecord.class, vs14.ArrayOfStringRecord.getClassSchema()},
        {vs15.ArrayOfStringRecord.class, vs15.ArrayOfStringRecord.getClassSchema()},
        {vs16.ArrayOfStringRecord.class, vs16.ArrayOfStringRecord.getClassSchema()},
        {vs17.ArrayOfStringRecord.class, vs17.ArrayOfStringRecord.getClassSchema()},
        {vs18.ArrayOfStringRecord.class, vs18.ArrayOfStringRecord.getClassSchema()},
        {vs19.ArrayOfStringRecord.class, vs19.ArrayOfStringRecord.getClassSchema()},
        {vs110.ArrayOfStringRecord.class, vs110.ArrayOfStringRecord.getClassSchema()},
        {vs111.ArrayOfStringRecord.class, vs111.ArrayOfStringRecord.getClassSchema()},

        {vs14.TestCollections.class, vs14.TestCollections.getClassSchema()},
        {vs15.TestCollections.class, vs15.TestCollections.getClassSchema()},
        {vs16.TestCollections.class, vs16.TestCollections.getClassSchema()},
        {vs17.TestCollections.class, vs17.TestCollections.getClassSchema()},
        {vs18.TestCollections.class, vs18.TestCollections.getClassSchema()},
        {vs19.TestCollections.class, vs19.TestCollections.getClassSchema()},
        {vs110.TestCollections.class, vs110.TestCollections.getClassSchema()},
        {vs111.TestCollections.class, vs111.TestCollections.getClassSchema()},

        {vs14.BuilderTester.class, vs14.BuilderTester.getClassSchema()},
        {vs15.BuilderTester.class, vs15.BuilderTester.getClassSchema()},
        {vs16.BuilderTester.class, vs16.BuilderTester.getClassSchema()},
        {vs17.BuilderTester.class, vs17.BuilderTester.getClassSchema()},
        {vs18.BuilderTester.class, vs18.BuilderTester.getClassSchema()},
        {vs19.BuilderTester.class, vs19.BuilderTester.getClassSchema()},
        {vs110.BuilderTester.class, vs110.BuilderTester.getClassSchema()},
        {vs111.BuilderTester.class, vs111.BuilderTester.getClassSchema()},

        {charseqmethod.TestCollections.class, charseqmethod.TestCollections.getClassSchema()}
    };
  }

  // Test round trip serialization using RandomRecordGenerator( put by index)
  @Test(dataProvider = "TestRoundTripSerializationProvider")
  public <T extends IndexedRecord> void testRoundTripSerialization(Class<T> clazz, org.apache.avro.Schema classSchema) throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    T instance = generator.randomSpecific(clazz, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    byte[] serialized = AvroCodecUtil.serializeBinary(instance);
    T deserialized = AvroCodecUtil.deserializeAsSpecific(serialized, classSchema, clazz);
    Assert.assertNotSame(deserialized, instance);
    compareIndexedRecords(deserialized, instance);
  }

  private void compareIndexedRecords(IndexedRecord record1, IndexedRecord record2) {
    try {
      Assert.assertEquals(record1, record2);
    } catch (AvroRuntimeException e) {
      if(e.getMessage().equals("Can't compare maps!")) {
        for(int i = 0; i < record1.getSchema().getFields().size(); i++) {
          Object fromDeserialized = record1.get(i);
          try {
            assertCollectionEquality(fromDeserialized, record2.get(i));
          } catch (AvroRuntimeException e2) {
            if(e2.getMessage().equals("Can't compare maps!")) {
              // then these must be records with maps inside
              compareIndexedRecords((IndexedRecord) fromDeserialized, (IndexedRecord) record2.get(i));
            } else {
              throw e2;
            }
          }
        }
      } else {
        throw e;
      }
    }
  }

  private void assertCollectionEquality(Object fromDeserialized, Object fromExpected) {
    if(fromDeserialized instanceof Map) {
      for(Object entry : ((Map) fromDeserialized).entrySet()) {
        CharSequence key = (CharSequence) ((Map.Entry) entry).getKey();
        Assert.assertTrue(((Map)fromExpected).containsKey(key));
        assertCollectionEquality(((Map) fromDeserialized).get(key), ((Map)fromExpected).get(key));
      }
    } else if(fromDeserialized instanceof List) {
      int i = 0;
      for(Object obj1 : (List) fromDeserialized) {
        Object obj2 = ((List)fromExpected).get(i++);
        assertCollectionEquality(obj1, obj2);
      }
    } else {
      Assert.assertEquals(fromDeserialized, fromExpected);
    }
  }

  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider14() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs14.Amount amount1 = generator.randomSpecific(vs14.Amount.class);
    vs14.Amount amount2 = generator.randomSpecific(vs14.Amount.class);
    vs14.Amount amount3 = generator.randomSpecific(vs14.Amount.class);

    vs14.RandomFixedName fixedName = generator.randomSpecific(vs14.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs14.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs14.Amount wierdUnionVal5 = generator.randomSpecific(vs14.Amount.class);;
    vs14.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs14.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");


    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider14")
  public void testSpecificRecordBuilder14(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs14.Amount min, List<vs14.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs14.Amount> mapOfRecord, vs14.Amount simpleUnion,
      vs14.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs14.BuilderTester builderTester = vs14.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs14.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs14.BuilderTester.getClassSchema(), vs14.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }

  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider15() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs15.Amount amount1 = generator.randomSpecific(vs15.Amount.class);
    vs15.Amount amount2 = generator.randomSpecific(vs15.Amount.class);
    vs15.Amount amount3 = generator.randomSpecific(vs15.Amount.class);

    vs15.RandomFixedName fixedName = generator.randomSpecific(vs15.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs15.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs15.Amount wierdUnionVal5 = generator.randomSpecific(vs15.Amount.class);;
    vs15.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs15.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");

    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider15")
  public void testSpecificRecordBuilder15(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs15.Amount min, List<vs15.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs15.Amount> mapOfRecord, vs15.Amount simpleUnion,
      vs15.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs15.BuilderTester builderTester = vs15.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs15.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs15.BuilderTester.getClassSchema(), vs15.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }

  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider16() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs16.Amount amount1 = generator.randomSpecific(vs16.Amount.class);
    vs16.Amount amount2 = generator.randomSpecific(vs16.Amount.class);
    vs16.Amount amount3 = generator.randomSpecific(vs16.Amount.class);

    vs16.RandomFixedName fixedName = generator.randomSpecific(vs16.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs16.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs16.Amount wierdUnionVal5 = generator.randomSpecific(vs16.Amount.class);;
    vs16.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs16.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");


    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider16")
  public void testSpecificRecordBuilder16(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs16.Amount min, List<vs16.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs16.Amount> mapOfRecord, vs16.Amount simpleUnion,
      vs16.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs16.BuilderTester builderTester = vs16.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs16.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs16.BuilderTester.getClassSchema(), vs16.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }

  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider17() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs17.Amount amount1 = generator.randomSpecific(vs17.Amount.class);
    vs17.Amount amount2 = generator.randomSpecific(vs17.Amount.class);
    vs17.Amount amount3 = generator.randomSpecific(vs17.Amount.class);

    vs17.RandomFixedName fixedName = generator.randomSpecific(vs17.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs17.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs17.Amount wierdUnionVal5 = generator.randomSpecific(vs17.Amount.class);;
    vs17.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs17.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");

    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider17")
  public void testSpecificRecordBuilder17(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs17.Amount min, List<vs17.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs17.Amount> mapOfRecord, vs17.Amount simpleUnion,
      vs17.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs17.BuilderTester builderTester = vs17.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs17.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs17.BuilderTester.getClassSchema(), vs17.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }
  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider18() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs18.Amount amount1 = generator.randomSpecific(vs18.Amount.class);
    vs18.Amount amount2 = generator.randomSpecific(vs18.Amount.class);
    vs18.Amount amount3 = generator.randomSpecific(vs18.Amount.class);

    vs18.RandomFixedName fixedName = generator.randomSpecific(vs18.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs18.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs18.Amount wierdUnionVal5 = generator.randomSpecific(vs18.Amount.class);;
    vs18.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs18.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");

    vs18.TestCollections testCollections1 = generator.randomSpecific(vs18.TestCollections.class);
    vs18.TestCollections testCollections2 = generator.randomSpecific(vs18.TestCollections.class);
    vs18.TestCollections testCollections3 = generator.randomSpecific(vs18.TestCollections.class);
    vs18.TestCollections testCollections4 = generator.randomSpecific(vs18.TestCollections.class);

    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider18")
  public void testSpecificRecordBuilder18(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs18.Amount min, List<vs18.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs18.Amount> mapOfRecord, vs18.Amount simpleUnion,
      vs18.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs18.BuilderTester builderTester = vs18.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs18.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs18.BuilderTester.getClassSchema(), vs18.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }


  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider19() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs19.Amount amount1 = generator.randomSpecific(vs19.Amount.class);
    vs19.Amount amount2 = generator.randomSpecific(vs19.Amount.class);
    vs19.Amount amount3 = generator.randomSpecific(vs19.Amount.class);

    vs19.RandomFixedName fixedName = generator.randomSpecific(vs19.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs19.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs19.Amount wierdUnionVal5 = generator.randomSpecific(vs19.Amount.class);;
    vs19.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs19.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");

    vs19.TestCollections testCollections1 = generator.randomSpecific(vs19.TestCollections.class);
    vs19.TestCollections testCollections2 = generator.randomSpecific(vs19.TestCollections.class);
    vs19.TestCollections testCollections3 = generator.randomSpecific(vs19.TestCollections.class);
    vs19.TestCollections testCollections4 = generator.randomSpecific(vs19.TestCollections.class);

    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider19")
  public void testSpecificRecordBuilder19(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs19.Amount min, List<vs19.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs19.Amount> mapOfRecord, vs19.Amount simpleUnion,
      vs19.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs19.BuilderTester builderTester = vs19.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs19.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs19.BuilderTester.getClassSchema(), vs19.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }

  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider110() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs110.Amount amount1 = generator.randomSpecific(vs110.Amount.class);
    vs110.Amount amount2 = generator.randomSpecific(vs110.Amount.class);
    vs110.Amount amount3 = generator.randomSpecific(vs110.Amount.class);

    vs110.RandomFixedName fixedName = generator.randomSpecific(vs110.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs110.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs110.Amount wierdUnionVal5 = generator.randomSpecific(vs110.Amount.class);;
    vs110.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs110.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");

    vs110.TestCollections testCollections1 = generator.randomSpecific(vs110.TestCollections.class);
    vs110.TestCollections testCollections2 = generator.randomSpecific(vs110.TestCollections.class);
    vs110.TestCollections testCollections3 = generator.randomSpecific(vs110.TestCollections.class);
    vs110.TestCollections testCollections4 = generator.randomSpecific(vs110.TestCollections.class);

    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider110")
  public void testSpecificRecordBuilder110(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs110.Amount min, List<vs110.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs110.Amount> mapOfRecord, vs110.Amount simpleUnion,
      vs110.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs110.BuilderTester builderTester = vs110.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs110.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs110.BuilderTester.getClassSchema(), vs110.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }
  @DataProvider
  private Object[][] testSpecificRecordBuilderProvider111() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs111.Amount amount1 = generator.randomSpecific(vs111.Amount.class);
    vs111.Amount amount2 = generator.randomSpecific(vs111.Amount.class);
    vs111.Amount amount3 = generator.randomSpecific(vs111.Amount.class);

    vs111.RandomFixedName fixedName = generator.randomSpecific(vs111.RandomFixedName.class);

    Map<String, String> stringMap = new HashMap<>();
    Map<String, vs111.Amount> amountMap = new HashMap<>();
    stringMap.put("isTrue", "false");
    amountMap.put("amount3", amount3);

    int wierdUnionVal2 = 2;
    long wierdUnionVal3 = 4L;
    String wierdUnionVal4 = "WierdVal";
    vs111.Amount wierdUnionVal5 = generator.randomSpecific(vs111.Amount.class);;
    vs111.RandomFixedName wierdUnionVal6 = generator.randomSpecific(vs111.RandomFixedName.class);
    List<String> wierdUnionVal7 = Arrays.asList("item1, item2");


    return new Object[][]{
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, null, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal2,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal3, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal4,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal5,
            Arrays.asList("123")
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal6, null
        },
        {
            "str", "pck", Float.valueOf("1"), Double.valueOf("2"), false, Arrays.asList("123", "123"), amount1,
            Arrays.asList(amount1, amount2), stringMap, amountMap, null, fixedName, wierdUnionVal7,
            Arrays.asList("123")
        }
    };
  }

  @Test(dataProvider = "testSpecificRecordBuilderProvider111")
  public void testSpecificRecordBuilder111(String stringField, String package$, Float exception, Double dbl,
      Boolean isTrue, List<String> arrayOfStrings, vs111.Amount min, List<vs111.Amount> arrayOfRecord,
      Map<String, String> mapOfStrings, Map<String, vs111.Amount> mapOfRecord, vs111.Amount simpleUnion,
      vs111.RandomFixedName fixedType, Object wierdUnion, List<String> unionOfArray) throws Exception {
    vs111.BuilderTester builderTester = vs111.BuilderTester.newBuilder()
        .setStringField(stringField)
        .setPackage$(package$)
        .setException(exception)
        .setDbl(dbl)
        .setIsTrue(isTrue)
        .setArrayOfStrings(arrayOfStrings)
        .setMin(min)
        .setArrayOfRecord(arrayOfRecord)
        .setMapOfStrings(mapOfStrings)
        .setMapOfRecord(mapOfRecord)
        .setSimpleUnion(simpleUnion)
        .setFixedType(fixedType)
        .setWierdUnion(wierdUnion)
        .setUnionOfArray(unionOfArray)
        .build();
    Assert.assertNotNull(builderTester);

    Assert.assertSame(builderTester.get(0), stringField);
    Assert.assertSame(builderTester.get(1), package$);
    Assert.assertSame(builderTester.get(6), min);
    Assert.assertSame(builderTester.get(7), arrayOfRecord);
    Assert.assertSame(builderTester.get(10), simpleUnion);
    Assert.assertSame(builderTester.get(11), fixedType);
    Assert.assertEquals(builderTester.get(2), exception);
    Assert.assertEquals(builderTester.get(3), dbl);
    Assert.assertEquals(builderTester.get(4), isTrue);

    // Use transformers to return a copy of data
    assertNotSameIfNotNull(builderTester.get(5), arrayOfStrings);
    Assert.assertEquals(builderTester.get(5), arrayOfStrings);
    assertNotSameIfNotNull(builderTester.get(8), mapOfStrings);
    Assert.assertEquals(builderTester.get(8), mapOfStrings);
    assertNotSameIfNotNull(builderTester.get(9), mapOfRecord);
    Assert.assertEquals(builderTester.get(9), mapOfRecord);
    assertNotSameIfNotNull(builderTester.get(13), unionOfArray);
    Assert.assertEquals(builderTester.get(13), unionOfArray);

    if(wierdUnion instanceof List && wierdUnion != null && ((List)wierdUnion).get(0) instanceof CharSequence) {
      Assert.assertEquals(builderTester.get(12), wierdUnion);
      Assert.assertNotSame(builderTester.get(12), wierdUnion);
      // if List<CharSeq>, runtime value in public var should be Utf8
      Assert.assertTrue(((List)builderTester.wierdUnion).get(0) instanceof Utf8);
      //but getter should be string
      Assert.assertTrue(((List)builderTester.getWierdUnion()).get(0) instanceof String);
    } else {
      Assert.assertSame(builderTester.get(12), wierdUnion);
    }

    //test runtime type of String
    Assert.assertTrue(builderTester.stringField instanceof Utf8);

    //runtime type of List of String should be List<Utf8>
    if(builderTester.arrayOfStrings != null) {
      for(CharSequence c : builderTester.arrayOfStrings) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    if(builderTester.unionOfArray != null) {
      for(CharSequence c : builderTester.unionOfArray) {
        Assert.assertTrue(c instanceof Utf8);
      }
    }

    // Test round trip serialization when builder is used
    byte[] serialized = AvroCodecUtil.serializeBinary(builderTester);
    vs111.BuilderTester deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs111.BuilderTester.getClassSchema(), vs111.BuilderTester.class);
    Assert.assertNotSame(deserialized, builderTester);
    compareIndexedRecords(deserialized, builderTester);

  }

  @Test
  public void TestCharSeqAccessor() {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs14.SimpleRecord instance14 = generator.randomSpecific(vs14.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    vs19.SimpleRecord instance19 = generator.randomSpecific(vs19.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string14 = "new_String_14";
    String string19 = "new_String_19";

    instance14.put(0, string14);
    instance19.setStringField(string19);

    Assert.assertTrue(instance14.stringField instanceof Utf8);
    Assert.assertEquals(instance14.stringField.toString(), string14);
    Assert.assertTrue(instance14.getStringField() instanceof String);
    Assert.assertEquals(instance14.stringField, new Utf8(instance14.getStringField()));

    Assert.assertTrue(instance19.stringField instanceof Utf8);
    Assert.assertEquals(instance19.stringField.toString(), string19);
    Assert.assertTrue(instance19.getStringField() instanceof String);
    Assert.assertEquals(instance19.stringField, new Utf8(instance19.getStringField()));

    vs15.SimpleRecord instance15 = generator.randomSpecific(vs15.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string15 = "new_String_15";
    instance15.setStringField(string15);
    Assert.assertTrue(instance15.stringField instanceof Utf8);
    Assert.assertEquals(instance15.stringField.toString(), string15);
    Assert.assertTrue(instance15.getStringField() instanceof String);
    Assert.assertEquals(instance15.stringField, new Utf8(instance15.getStringField()));

    vs16.SimpleRecord instance16 = generator.randomSpecific(vs16.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string16 = "new_String_16";
    instance16.setStringField(string16);
    Assert.assertTrue(instance16.stringField instanceof Utf8);
    Assert.assertEquals(instance16.stringField.toString(), string16);
    Assert.assertTrue(instance16.getStringField() instanceof String);
    Assert.assertEquals(instance16.stringField, new Utf8(instance16.getStringField()));

    vs17.SimpleRecord instance17 = generator.randomSpecific(vs17.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string17 = "new_String_17";
    instance17.setStringField(string17);
    Assert.assertTrue(instance17.stringField instanceof Utf8);
    Assert.assertEquals(instance17.stringField.toString(), string17);
    Assert.assertTrue(instance17.getStringField() instanceof String);
    Assert.assertEquals(instance17.stringField, new Utf8(instance17.getStringField()));

    vs18.SimpleRecord instance18 = generator.randomSpecific(vs18.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string18 = "new_String_18";
    instance18.setStringField(string18);
    Assert.assertTrue(instance18.stringField instanceof Utf8);
    Assert.assertEquals(instance18.stringField.toString(), string18);
    Assert.assertTrue(instance18.getStringField() instanceof String);
    Assert.assertEquals(instance18.stringField, new Utf8(instance18.getStringField()));

    vs110.SimpleRecord instance110 = generator.randomSpecific(vs110.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string110 = "new_String_110";
    instance110.setStringField(string110);
    Assert.assertTrue(instance110.stringField instanceof Utf8);
    Assert.assertEquals(instance110.stringField.toString(), string110);
    Assert.assertTrue(instance110.getStringField() instanceof String);
    Assert.assertEquals(instance110.stringField, new Utf8(instance110.getStringField()));

    vs111.SimpleRecord instance111 = generator.randomSpecific(vs111.SimpleRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    String string111 = "new_String_111";
    instance111.setStringField(string111);
    Assert.assertTrue(instance111.stringField instanceof Utf8);
    Assert.assertEquals(instance111.stringField.toString(), string111);
    Assert.assertTrue(instance111.getStringField() instanceof String);
    Assert.assertEquals(instance111.stringField, new Utf8(instance111.getStringField()));

  }

  @DataProvider
  private Object[][] testStringTypeParamsProvider() {
    Map<String, String> vs14TestCollectionsFieldToType = new LinkedHashMap<String, String>() {{
      put("str", "class java.lang.String");
      put("strAr", "java.util.List<java.lang.String>");
      put("strArAr", "java.util.List<java.util.List<java.lang.String>>");
      put("unionOfArray", "java.util.List<java.lang.String>");
      put("arOfMap", "java.util.List<java.util.Map<java.lang.String, java.lang.String>>");
      put("unionOfMap", "java.util.Map<java.lang.String, java.lang.String>");
      put("arOfUnionOfStr", "java.util.List<java.lang.String>");
      put("arOfMapOfUnionOfArray", "java.util.List<java.util.Map<java.lang.String, java.util.List<java.lang.String>>>");
    }};

    Map<String, String> vs14TestCollectionsCharSeqFieldToType = new LinkedHashMap<String, String>() {{
      put("str", "interface java.lang.CharSequence");
      put("strAr", "java.util.List<java.lang.CharSequence>");
      put("strArAr", "java.util.List<java.util.List<java.lang.CharSequence>>");
      put("unionOfArray", "java.util.List<java.lang.CharSequence>");
      put("arOfMap", "java.util.List<java.util.Map<java.lang.CharSequence, java.lang.CharSequence>>");
      put("unionOfMap", "java.util.Map<java.lang.CharSequence, java.lang.CharSequence>");
      put("arOfUnionOfStr", "java.util.List<java.lang.CharSequence>");
      put("arOfMapOfUnionOfArray", "java.util.List<java.util.Map<java.lang.CharSequence, java.util.List<java.lang.CharSequence>>>");
    }};

    return new Object[][]{
        {vs14.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs14.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs15.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs15.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs16.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs16.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs17.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs17.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs18.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs18.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs19.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs19.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs110.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs110.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {vs111.TestCollections.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, false},
        {vs111.TestCollections.Builder.class, vs14TestCollectionsFieldToType, vs14TestCollectionsCharSeqFieldToType, true},
        {charseqmethod.TestCollections.class, vs14TestCollectionsCharSeqFieldToType, vs14TestCollectionsFieldToType, false},
        {charseqmethod.TestCollections.Builder.class, vs14TestCollectionsCharSeqFieldToType, vs14TestCollectionsFieldToType, true}
    };
  }

  @Test(dataProvider = "testStringTypeParamsProvider")
  public void testStringTypeParams(Class<?> clazz, Map<String, String> fieldToType, Map<String, String> fieldToType2ndCtr, boolean isBuilder) throws NoSuchMethodException {

    if(!isBuilder) {
      List<Constructor> constructors = Arrays.stream(clazz.getConstructors()).filter(constructor -> constructor.getParameters().length != 0).collect(
          Collectors.toList());
      List<List<String>> listOfListOfConstructorParamsExpected =
          Arrays.asList(new ArrayList<>(fieldToType.values()), new ArrayList<>(fieldToType2ndCtr.values()));
      List<List<String>> listOfListOfConstructorParamsActual = new ArrayList<>();
      for(Constructor constructor : constructors) {
        listOfListOfConstructorParamsActual.add(
            Arrays.stream(constructor.getParameters()).map(param -> param.getParameterizedType().toString()).collect(
                Collectors.toList())
        );
      }
      Assert.assertTrue((listOfListOfConstructorParamsExpected.get(0).equals(listOfListOfConstructorParamsActual.get(0))
          && listOfListOfConstructorParamsExpected.get(1).equals(listOfListOfConstructorParamsActual.get(1)))
          || (listOfListOfConstructorParamsExpected.get(1).equals(listOfListOfConstructorParamsActual.get(0))
          && listOfListOfConstructorParamsExpected.get(0).equals(listOfListOfConstructorParamsActual.get(1))));
    }

    List<String> setterMethodNames = fieldToType.keySet().stream().map(fieldName -> getMethodWithPrefixForField(fieldName, "set")).collect(Collectors.toList());
    List<String> getterMethodNames = fieldToType.keySet().stream().map(fieldName -> getMethodWithPrefixForField(fieldName, "get")).collect(Collectors.toList());

    Map<String, String> getterMethodsTypes = Arrays.stream(clazz.getMethods())
        .filter(method -> getterMethodNames.contains(method.getName()))
        .collect(Collectors.toMap(Method::getName, method -> method.getAnnotatedReturnType().getType().toString()));

    // Setters for String + Charseq types should be present
    // builder only have setters defined by defaultMethodStringRep in config
    if (isBuilder) {
      Map<String, String> setterMethodsTypesMap = Arrays.stream(clazz.getMethods())
          .filter(method -> setterMethodNames.contains(method.getName()))
          .collect(
              Collectors.toMap(Method::getName, method -> method.getParameters()[0].getParameterizedType().toString()));

      for (String fieldName : fieldToType.keySet()) {
        Assert.assertEquals(fieldToType.get(fieldName),
            setterMethodsTypesMap.get(getMethodWithPrefixForField(fieldName, "set")));
      }
    } else {
      List<String> setterMethodsTypes = Arrays.stream(clazz.getMethods())
          .filter(method -> setterMethodNames.contains(method.getName()))
          .map(method -> method.getName()+ "::" + method.getParameters()[0].getParameterizedType().toString())
          .collect(Collectors.toList());
      List<String> setterMethodTypesExpected = fieldToType.entrySet()
          .stream()
          .map(entry -> getMethodWithPrefixForField(entry.getKey(), "set") + "::" + entry.getValue())
          .collect(Collectors.toList());
      if(fieldToType.get("str").equals("class java.lang.String")) {
        setterMethodTypesExpected.add(getMethodWithPrefixForField("str", "set") + "::" + "interface java.lang.CharSequence");
      } else {
        setterMethodTypesExpected.add(getMethodWithPrefixForField("str", "set") + "::" + "class java.lang.String");
      }

      Collections.sort(setterMethodTypesExpected);
      Collections.sort(setterMethodsTypes);

      Assert.assertEquals(setterMethodsTypes, setterMethodTypesExpected);
    }

    for(String fieldName : fieldToType.keySet()) {
      Assert.assertEquals(fieldToType.get(fieldName), getterMethodsTypes.get(getMethodWithPrefixForField(fieldName, "get")));
    }

  }

  private String getMethodWithPrefixForField(String fieldName, String prefix) {
    return prefix + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
  }

  @Test
  public void testRecordWithCharSeqStringTypeForMethods() throws Exception {
    CharSequence str = "str";
    Map<CharSequence, CharSequence> mapCharSeq  = new HashMap<CharSequence, CharSequence>() {{
      put("key1", "value1");
      put("key2", "value2");
    }};

    Map<CharSequence, List<CharSequence>> mapOfList = new HashMap<CharSequence, List<CharSequence>>() {{
      put("key1", Arrays.asList("val1", "val2"));
      put("key2", Arrays.asList("val10", "val20"));
    }};
    charseqmethod.TestCollections.Builder testCollectionsBuilder = charseqmethod.TestCollections.newBuilder()
        .setStr(str)
        .setStrAr(Arrays.asList(str))
        .setStrArAr(Arrays.asList(Arrays.asList(str), Arrays.asList(str)))
        .setUnionOfArray(Arrays.asList(str))
        .setArOfMap(Arrays.asList(mapCharSeq))
        .setUnionOfMap(mapCharSeq)
        .setArOfUnionOfStr(Arrays.asList(str))
        .setArOfMapOfUnionOfArray(Arrays.asList(mapOfList));

    charseqmethod.TestCollections testCollections = testCollectionsBuilder.build();

    Assert.assertTrue(testCollections.get(0) instanceof CharSequence);

    Assert.assertTrue(testCollections.get(1) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(1)).get(0) instanceof CharSequence);

    Assert.assertTrue(testCollections.get(2) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(2)).get(0) instanceof List);
    Assert.assertTrue(((List) ((List<?>) testCollections.get(2)).get(0)).get(0) instanceof CharSequence);

    Assert.assertTrue(testCollections.get(3) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(3)).get(0) instanceof CharSequence);

    Assert.assertTrue(testCollections.get(4) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(4)).get(0) instanceof Map);
    Set<Map.Entry<CharSequence, CharSequence>> entrySet = ((Map) ((List) testCollections.get(4)).get(0)).entrySet();
    for(Map.Entry entry : entrySet) {
      Assert.assertTrue(entry.getKey() instanceof CharSequence);
      Assert.assertFalse(entry.getKey() instanceof Utf8);
      Assert.assertTrue(entry.getValue() instanceof CharSequence);
      Assert.assertFalse(entry.getValue() instanceof Utf8);
    }
    Assert.assertTrue(testCollections.get(5) instanceof Map);
    //Validates all entry sets are in CharSeq->CharSeq form
    Set<Map.Entry<CharSequence, CharSequence>> entrySet2 = ((Map) testCollections.get(5)).entrySet();
    for(Map.Entry entry : entrySet2) {
      Assert.assertTrue(entry.getKey() instanceof CharSequence);
      Assert.assertFalse(entry.getKey() instanceof Utf8);
      Assert.assertTrue(entry.getValue() instanceof CharSequence);
      Assert.assertFalse(entry.getValue() instanceof Utf8);
    }

    Assert.assertTrue(testCollections.get(6) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(6)).get(0) instanceof CharSequence);

    Assert.assertTrue(testCollections.get(7) instanceof List);
    Assert.assertTrue(((List<?>) testCollections.get(7)).get(0) instanceof Map);

    Set<Map.Entry<CharSequence, List<CharSequence>>> entrySet4 = ((Map) ((List<?>) testCollections.get(7)).get(0)).entrySet();
    for(Map.Entry<CharSequence, List<CharSequence>> entry : entrySet4) {
      Assert.assertTrue(((Map.Entry)entry).getKey() instanceof CharSequence);
      Assert.assertFalse(((Map.Entry)entry).getKey() instanceof Utf8);

      Assert.assertTrue(((Map.Entry)entry).getValue() instanceof List);
      Assert.assertTrue(((List<?>) ((Map.Entry)entry).getValue()).get(0) instanceof CharSequence);
      Assert.assertFalse(((List<?>) ((Map.Entry)entry).getValue()).get(0) instanceof Utf8);
    }

    Assert.assertTrue(testCollections.str instanceof Utf8);
    Assert.assertTrue(((List<?>) testCollections.strAr).get(0) instanceof Utf8);
    Assert.assertTrue(((List) ((List<?>) testCollections.strArAr).get(0)).get(0) instanceof Utf8);
    Assert.assertTrue(((List<?>) testCollections.unionOfArray).get(0) instanceof CharSequence);
    Set<Map.Entry<Utf8, Utf8>> entrySet3 = ((Map) testCollections.unionOfMap).entrySet();
    for(Map.Entry entry : entrySet3) {
      Assert.assertTrue(entry.getKey() instanceof Utf8);
      Assert.assertTrue(entry.getValue() instanceof Utf8);
    }

    Assert.assertTrue(((List<?>) testCollections.arOfUnionOfStr).get(0) instanceof Utf8);

    Set<Map.Entry<Utf8, List<Utf8>>> entrySet5 = ((Map) ((List<?>) testCollections.arOfMapOfUnionOfArray).get(0)).entrySet();
    for(Map.Entry<Utf8, List<Utf8>> entry : entrySet5) {
      Assert.assertTrue(((Map.Entry)entry).getKey() instanceof CharSequence);
      Assert.assertTrue(((Map.Entry)entry).getKey() instanceof Utf8);

      Assert.assertTrue(((Map.Entry)entry).getValue() instanceof List);
      Assert.assertTrue(((List<?>) ((Map.Entry)entry).getValue()).get(0) instanceof CharSequence);
      Assert.assertTrue(((List<?>) ((Map.Entry)entry).getValue()).get(0) instanceof Utf8);
    }
  }

  @DataProvider
  private Object[][] testRecordWitNoSimpleStrConstructorProvider() {
    return new Object[][]{
        {vs14.HasNoSimpleString.class},
        {vs15.HasNoSimpleString.class},
        {vs16.HasNoSimpleString.class},
        {vs17.HasNoSimpleString.class},
        {vs18.HasNoSimpleString.class},
        {vs19.HasNoSimpleString.class},
        {vs110.HasNoSimpleString.class},
        {vs111.HasNoSimpleString.class},

        //Default method type charseq
        {charseqmethod.HasNoSimpleString.class}

    };
  }

  @Test(dataProvider = "testRecordWitNoSimpleStrConstructorProvider")
  public void testRecordWitNoSimpleStrConstructor(Class<?> clazz) {

    List<Constructor> constructors = Arrays.stream(clazz.getConstructors()).filter(constructor -> constructor.getParameters().length != 0).collect(
        Collectors.toList());
    Assert.assertEquals(constructors.size(), 1);
  }

  private void assertNotSameIfNotNull(Object obj1, Object obj2) {
    if(obj1 != null) {
      Assert.assertNotSame(obj1, obj2);
    }
  }

  @BeforeClass
  public void setup() {
    System.setProperty("org.apache.avro.specific.use_custom_coders", "true");
  }
}
