/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.google.common.base.Throwables;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests the default value related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperDefaultsTest {

  @Test
  public void testGetSpecificFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("nullField")));
    Assert.assertTrue((Boolean) AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("boolField")));
    //returns a Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("strField")).toString(), "default");
    Assert.assertNull(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithNullDefault")));
    //Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithStringDefault")).toString(), "def");
  }

  @Test
  public void testGetGenericFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("nullField")));
    Assert.assertTrue((Boolean) AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("boolField")));
    //returns a Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("strField")).toString(), "default");
    Assert.assertNull(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithNullDefault")));
    //Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithStringDefault")).toString(), "def");
  }

  @Test
  public void testGetCompatibleDefaultValue() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("nullField")));
    Assert.assertTrue((Boolean) AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("boolField")));
    //returns a Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("strField")).toString(), "default");
    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("unionWithNullDefault")));
    //Utf8
    Assert.assertEquals(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("unionWithStringDefault")).toString(), "def");
  }

  @Test
  public void testGetFieldDefaultsAsJson() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("nullField")), "null");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("boolField")), "true");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("strField")), "\"default\"");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithNullDefault")), "null");
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithStringDefault")), "\"def\"");

    schema = by14.HasComplexDefaults.SCHEMA$;
    Assert.assertEquals(AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("fieldWithDefaultRecord")), "{\"intField\":7}");
  }

  @Test
  public void testGetDefaultsForFieldsWithoutDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("nullWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("nullWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("nullWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("nullWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("boolWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("boolWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("boolWithoutDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("boolWithoutDefault"));
    }

    try {
      AvroCompatibilityHelper.getGenericDefaultValue(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }

    try {
      AvroCompatibilityHelper.getSpecificDefaultValue(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }

    try {
      AvroCompatibilityHelper.getDefaultValueAsJsonString(schema.getField("unionWithStringNoDefault"));
    } catch (AvroRuntimeException expected) {
      Throwable root = Throwables.getRootCause(expected);
      Assert.assertTrue(root.getMessage().contains("unionWithStringNoDefault"));
    }
  }

  @Test
  public void testGetCompatibleDefaultValueWithoutDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("nullWithoutDefault")));
    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("boolWithoutDefault")));
    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(schema.getField("unionWithStringNoDefault")));
  }

  @Test
  public void testComplexDefaultValue() throws Exception {
    Schema schema = under14.HasComplexDefaults.SCHEMA$;

    Schema.Field field = schema.getField("fieldWithDefaultEnum");
    Object specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultEnum);
    Object genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.EnumSymbol);

    field = schema.getField("fieldWithDefaultFixed");
    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultFixed);
    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Fixed);

    field = schema.getField("fieldWithDefaultRecord");
    specificDefault = AvroCompatibilityHelper.getSpecificDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof under14.DefaultRecord);
    genericDefault = AvroCompatibilityHelper.getGenericDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Record);
  }

  @Test
  public void testBadDefaultValues() throws Exception {
    List<Schema.Type> primitives = Arrays.asList(
        Schema.Type.NULL,
        Schema.Type.BOOLEAN,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.STRING,
        Schema.Type.BYTES
    );
    //key is type, values are "equivalent" types
    Map<Schema.Type, List<Schema.Type>> types = new LinkedHashMap<>();
    types.put(Schema.Type.NULL, Arrays.asList(Schema.Type.NULL));
    types.put(Schema.Type.BOOLEAN, Arrays.asList(Schema.Type.BOOLEAN));
    types.put(Schema.Type.INT, Arrays.asList(Schema.Type.INT, Schema.Type.LONG));
    types.put(Schema.Type.LONG, Arrays.asList(Schema.Type.INT, Schema.Type.LONG));
    types.put(Schema.Type.FLOAT, Arrays.asList(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE));
    types.put(Schema.Type.DOUBLE, Arrays.asList(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE));
    types.put(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING, Schema.Type.BYTES));
    types.put(Schema.Type.BYTES, Arrays.asList(Schema.Type.STRING, Schema.Type.BYTES));

    for (Schema.Type type : types.keySet()) {
      List<Schema.Type> equivalents = types.get(type);
      List<Schema.Type> badValueCandidates = new ArrayList<>(primitives);
      List<Schema.Type> goodValueCandidates = new ArrayList<>(primitives);
      badValueCandidates.removeAll(equivalents);
      goodValueCandidates.removeAll(badValueCandidates);

      //do all the simple fields
      for (Schema.Type valueType : badValueCandidates) {
        String typeStr = type.name().toLowerCase(Locale.ROOT);
        String badTypeStr = valueType.name().toLowerCase(Locale.ROOT);
        String badLiteral = randomJsonLiteral(valueType);
        String fieldName = typeStr + "With" + badTypeStr + "Default";
        String avsc = "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": [{\"name\": \""
            + fieldName + "\", \"type\": \"" + typeStr + "\", \"default\": " + badLiteral + "}]}";
        runBadDefaultCycle(avsc, fieldName);
      }

      //now do arrays
      for (Schema.Type valueType : badValueCandidates) {
        String typeStr = type.name().toLowerCase(Locale.ROOT);
        String badTypeStr = valueType.name().toLowerCase(Locale.ROOT);
        String badLiteral = randomJsonLiteral(valueType);
        String fieldName = typeStr + "ArrayWith" + badTypeStr + "Default";
        String avsc = "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": [{\"name\": \""
            + fieldName + "\", \"type\": { \"type\": \"array\", \"items\": \"" + typeStr + "\"}, \"default\": " + badLiteral + "}]}";
        runBadDefaultCycle(avsc, fieldName);
      }

      //and arrays with array defaults
      for (Schema.Type valueType : badValueCandidates) {
        String typeStr = type.name().toLowerCase(Locale.ROOT);
        String badTypeStr = valueType.name().toLowerCase(Locale.ROOT);
        String badLiteral = randomArrayJsonLiteral(valueType);
        String fieldName = typeStr + "ArrayWith" + badTypeStr + "DefaultArray";
        String avsc = "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": [{\"name\": \""
            + fieldName + "\", \"type\": { \"type\": \"array\", \"items\": \"" + typeStr + "\"}, \"default\": " + badLiteral + "}]}";
        runBadDefaultCycle(avsc, fieldName);
      }
    }

    //throw in a few manual ones that random is unlikely to generate
    runBadDefaultCycle(
        "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": [{\"name\": \"intWithRoundFloatDefault\", \"type\": \"int\", \"default\": 5.0}]}",
        "intWithRoundFloatDefault"
    );
  }

  public static String randomArrayJsonLiteral(Schema.Type type) {
    Random random = new Random();
    int length = 1 + random.nextInt(5); //1-5 because an empty array is valid for all array types
    StringJoiner csv = new StringJoiner(", ");
    for (int i = 0; i < length; i++) {
      csv.add(randomJsonLiteral(type));
    }
    return "[" + csv.toString() + "]";
  }

  public static String randomJsonLiteral(Schema.Type type) {
    Random random = new Random();
    int length;
    StringBuilder sb;
    switch (type) {
      case NULL:
        return "null";
      case BOOLEAN:
        return random.nextBoolean() ? "true" : "false";
      case INT:
        return "" + random.nextInt(100);
      case LONG:
        return "" + ( ((long) Integer.MAX_VALUE) + 1 + random.nextInt(100) );
      case FLOAT:
        return "" + random.nextFloat();
      case DOUBLE:
        return "" + random.nextDouble();
      case STRING:
        length = random.nextInt(10); //0-9
        sb = new StringBuilder();
        sb.append("\"");
        for (int i = 0; i < length; i++) {
          int val = 64 + random.nextInt(10);
          sb.append((char) val);
        }
        sb.append("\"");
        return sb.toString();
      case BYTES:
        length = random.nextInt(10); //0-9
        sb = new StringBuilder();
        sb.append("\"");
        for (int i = 0; i < length; i++) {
          int val = 64 + random.nextInt(10);
          sb.append((char) val);
        }
        sb.append("\"");
        return sb.toString();
      default:
        throw new IllegalStateException("dont know how to generate a random " + type);
    }
  }

  public void runBadDefaultCycle(String avsc, String fieldName) throws Exception {
    Schema parsed;
    try {
      parsed = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.LOOSE, null).getMainSchema();
    } catch (NumberFormatException validatedByAvro) {
      //avro gets progressively better at validation. where it does, we cant test this
      return;
    } catch (Exception unexpected) {
      Assert.fail("avro refused to parse " + avsc, unexpected);
      return;
    }
    Schema.Field field = parsed.getField(fieldName);
    //strict should fail
    try {
      AvroCompatibilityHelper.getGenericDefaultValue(field);
      Assert.fail("should have thrown for " + avsc);
    } catch (AvroTypeException expected) {
      Assert.assertTrue(expected.getMessage().toLowerCase(Locale.ROOT).contains("invalid default"));
    }
    //the more forgiving method should just return null
    Assert.assertNull(AvroCompatibilityHelper.getNullableGenericDefaultValue(field));
  }
}
