package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.DefaultsEnum;
import com.linkedin.avro.fastserde.generated.avro.DefaultsFixed;
import com.linkedin.avro.fastserde.generated.avro.DefaultsNewEnum;
import com.linkedin.avro.fastserde.generated.avro.DefaultsSubRecord;
import com.linkedin.avro.fastserde.generated.avro.DefaultsTestRecord;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastDeserializerDefaultsTest {

  private File tempDir;
  private ClassLoader classLoader;

  @DataProvider(name = "SlowFastDeserializer")
  public static Object[][] deserializers() {
    return new Object[][]{{true}, {false}};
  }

  @BeforeTest(groups = {"deserializationTest"})
  public void prepare() throws Exception {
    Path tempPath = Files.createTempDirectory("generated");
    tempDir = tempPath.toFile();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastDeserializerDefaultsTest.class.getClassLoader());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  @SuppressWarnings("unchecked")
  public void shouldReadSpecificDefaults(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
    oldSubRecord.put("oldSubField", new Utf8("testValueOfSubField"));
    oldSubRecord.put("fieldToBeRemoved", 33);
    oldRecord.put("oldSubRecord", oldSubRecord);

    // when
    DefaultsTestRecord testRecord = null;
    if (whetherUseFastDeserializer.booleanValue()) {
      testRecord = decodeSpecificFast(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      testRecord = decodeSpecificSlow(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    Assert.assertEquals(oldSubRecord.get("oldSubField"), testRecord.oldSubRecord.oldSubField);
    Assert.assertEquals(new Utf8("defaultOldSubField"),
        testRecord.newFieldWithOldSubRecord.oldSubField);
    Assert.assertEquals(42, testRecord.testInt);
    Assert.assertNull(testRecord.testIntUnion);
    Assert.assertEquals(9223372036854775807L, testRecord.testLong);
    Assert.assertNull(testRecord.testLongUnion);
    Assert.assertEquals(3.14d, testRecord.testDouble, 0);
    Assert.assertNull(testRecord.testDoubleUnion);
    Assert.assertEquals(3.14f, testRecord.testFloat, 0);
    Assert.assertNull(testRecord.testFloatUnion);
    Assert.assertEquals(true, testRecord.testBoolean);
    Assert.assertNull(testRecord.testBooleanUnion);
    Assert.assertEquals(ByteBuffer.wrap("1234".getBytes()), testRecord.testBytes);
    Assert.assertNull(testRecord.testBytesUnion);
    Assert.assertEquals(new Utf8("testStringValue"), testRecord.testString);
    if (Utils.isAvro14()) {
      Assert.assertEquals(new Utf8("http://www.example.com"), testRecord.testStringable);
    } else {
      Assert.assertEquals(new URL("http://www.example.com"), testRecord.testStringable);
    }
    Assert.assertNull(testRecord.testStringUnion);
    DefaultsFixed expectedDefaultsFixed1 = new DefaultsFixed();
    expectedDefaultsFixed1.bytes(new byte[]{(byte) '5'});
    Assert.assertEquals(expectedDefaultsFixed1, testRecord.testFixed);
    Assert.assertNull(testRecord.testFixedUnion);
    DefaultsFixed expectedDefaultsFixed2 = new DefaultsFixed();
    expectedDefaultsFixed2.bytes(new byte[]{(byte) '6'});
    Assert.assertEquals(Collections.singletonList(expectedDefaultsFixed2), testRecord.testFixedArray);

    List listWithNull = new LinkedList();
    listWithNull.add(null);
    Assert.assertEquals(listWithNull, testRecord.testFixedUnionArray);
    Assert.assertEquals(DefaultsEnum.C, testRecord.testEnum);
    Assert.assertNull(testRecord.testEnumUnion);
    Assert.assertTrue(Arrays.asList(Arrays.asList(DefaultsNewEnum.B)).equals(testRecord.testNewEnumIntUnionArray));
    Assert.assertEquals(Arrays.asList(DefaultsEnum.E, DefaultsEnum.B), testRecord.testEnumArray);
    Assert.assertEquals(listWithNull, testRecord.testEnumUnionArray);
    Assert.assertNull(testRecord.subRecordUnion);
    DefaultsSubRecord expectedSubRecord1 = new DefaultsSubRecord();
    expectedSubRecord1.subField = "valueOfSubField";
    expectedSubRecord1.arrayField = Collections.singletonList(DefaultsEnum.A);
    Assert.assertEquals(expectedSubRecord1, testRecord.subRecord);

    DefaultsSubRecord expectedSubRecord2 = new DefaultsSubRecord();
    expectedSubRecord2.subField = "recordArrayValue";
    expectedSubRecord2.arrayField = Collections.singletonList(DefaultsEnum.A);
    Assert.assertEquals(Collections.singletonList(expectedSubRecord2), testRecord.recordArray);
    Assert.assertEquals(listWithNull, testRecord.recordUnionArray);

    Map stringableMap = new HashMap();
    // stringable is not supported in avro-1.4
    if (Utils.isAvro14()) {
      stringableMap.put(new Utf8("http://www.example2.com"), new Utf8("123"));
    } else {
      stringableMap.put(new URL("http://www.example2.com"), new BigInteger("123"));
    }
    Assert.assertEquals(stringableMap, testRecord.stringableMap);

    Map recordMap = new HashMap();
    DefaultsSubRecord subRecord3 = new DefaultsSubRecord();
    subRecord3.subField = "recordMapValue";
    subRecord3.arrayField = Collections.singletonList(DefaultsEnum.A);
    recordMap.put(new Utf8("test"), subRecord3);
    Assert.assertEquals(recordMap, testRecord.recordMap);

    Map recordUnionMap = new HashMap();
    recordUnionMap.put(new Utf8("test"), null);
    Assert.assertEquals(recordUnionMap, testRecord.recordUnionMap);
    Assert.assertEquals(Collections.singletonList(recordUnionMap), testRecord.recordUnionMapArray);

    Map recordUnionArrayMap = new HashMap();
    recordUnionArrayMap.put(new Utf8("test"), listWithNull);
    Assert.assertTrue(recordUnionArrayMap.equals(testRecord.recordUnionArrayMap));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  @SuppressWarnings("unchecked")
  public void shouldReadGenericDefaults(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
    oldSubRecord.put("oldSubField", new Utf8("testValueOfSubField"));
    oldSubRecord.put("fieldToBeRemoved", 33);
    oldRecord.put("oldSubRecord", oldSubRecord);

    // when
    GenericRecord testRecord = null;
    if (whetherUseFastDeserializer) {
      testRecord = decodeGenericFast(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      testRecord = decodeGenericSlow(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    Assert.assertEquals(oldSubRecord.get("oldSubField"),
        ((GenericData.Record) testRecord.get("oldSubRecord")).get("oldSubField"));
    Assert.assertEquals(new Utf8("defaultOldSubField"),
        ((GenericData.Record) testRecord.get("newFieldWithOldSubRecord")).get("oldSubField"));
    Assert.assertEquals(42, (int) testRecord.get("testInt"));
    Assert.assertNull(testRecord.get("testIntUnion"));
    Assert.assertEquals(9223372036854775807L, (long) testRecord.get("testLong"));
    Assert.assertNull(testRecord.get("testLongUnion"));
    Assert.assertEquals(3.14d, (double) testRecord.get("testDouble"), 0);
    Assert.assertNull(testRecord.get("testDoubleUnion"));
    Assert.assertEquals(3.14f, (float) testRecord.get("testFloat"), 0);
    Assert.assertNull(testRecord.get("testFloatUnion"));
    Assert.assertEquals(true, testRecord.get("testBoolean"));
    Assert.assertNull(testRecord.get("testBooleanUnion"));
    Assert.assertEquals(ByteBuffer.wrap("1234".getBytes()), testRecord.get("testBytes"));
    Assert.assertNull(testRecord.get("testBytesUnion"));
    Assert.assertEquals(new Utf8("testStringValue"), testRecord.get("testString"));
    Assert.assertEquals(new Utf8("http://www.example.com"), testRecord.get("testStringable"));

    Assert.assertNull(testRecord.get("testStringUnion"));
    Schema fixedSchema = Schema.createFixed("DefaultsFixed", "", "", 1);
    GenericData.Fixed expectedFixed1 = AvroCompatibilityHelper.newFixedField(fixedSchema, new byte[]{(byte) '5'});
    Assert.assertEquals(expectedFixed1, testRecord.get("testFixed"));
    Assert.assertNull(testRecord.get("testFixedUnion"));
    GenericData.Fixed expectedFixed2 = AvroCompatibilityHelper.newFixedField(fixedSchema, new byte[]{(byte) '6'});
    Assert.assertTrue(Arrays.asList(expectedFixed2).equals(testRecord.get("testFixedArray")));

    List listWithNull = new LinkedList();
    listWithNull.add(null);
    Assert.assertTrue(listWithNull.equals(testRecord.get("testFixedUnionArray")));
    Assert.assertEquals("C", testRecord.get("testEnum").toString());
    Assert.assertNull(testRecord.get("testEnumUnion"));
    Schema enumSchema = Schema.createEnum("DefaultsNewEnum", "", "", Arrays.asList("A", "B"));
    Assert.assertTrue(Arrays.asList(Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "B")))
        .equals(testRecord.get("testNewEnumIntUnionArray")));

    Assert.assertEquals("E", ((List<GenericData.EnumSymbol>) testRecord.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("B", ((List<GenericData.EnumSymbol>) testRecord.get("testEnumArray")).get(1).toString());
    Assert.assertTrue(listWithNull.equals(testRecord.get("testEnumUnionArray")));
    Assert.assertNull(testRecord.get("subRecordUnion"));
    Assert.assertEquals(newGenericSubRecord("valueOfSubField", null, "A"), testRecord.get("subRecord"));
    Assert.assertTrue(
        Arrays.asList(newGenericSubRecord("recordArrayValue", null, "A")).equals(testRecord.get("recordArray")));
    Assert.assertTrue(listWithNull.equals(testRecord.get("recordUnionArray")));

    Map stringableMap = new HashMap();
    stringableMap.put(new Utf8("http://www.example2.com"), new Utf8("123"));
    Assert.assertEquals(stringableMap, testRecord.get("stringableMap"));

    Map recordMap = new HashMap();
    recordMap.put(new Utf8("test"), newGenericSubRecord("recordMapValue", null, "A"));
    Assert.assertEquals(recordMap, testRecord.get("recordMap"));

    Map recordUnionMap = new HashMap();
    recordUnionMap.put(new Utf8("test"), null);
    Assert.assertEquals(recordUnionMap, testRecord.get("recordUnionMap"));
    Assert.assertTrue(
        new ArrayList(Collections.singletonList(recordUnionMap)).equals(testRecord.get("recordUnionMapArray")));

    Map recordUnionArrayMap = new HashMap();
    recordUnionArrayMap.put(new Utf8("test"), listWithNull);
    Assert.assertTrue(recordUnionArrayMap.equals(testRecord.get("recordUnionArrayMap")));
  }

  @Test(groups = {"deserializationTest"})
  public void shouldReadSpecificLikeSlow() throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
    oldSubRecord.put("oldSubField", "testValueOfSubField");
    oldSubRecord.put("fieldToBeRemoved", 33);
    oldRecord.put("oldSubRecord", oldSubRecord);

    // when
    DefaultsTestRecord testRecordSlow =
        decodeSpecificSlow(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    DefaultsTestRecord testRecordFast =
        decodeSpecificFast(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));

    // then
    // map compare is not available in avro-1.4
    System.out.println(testRecordFast);
    System.out.println(testRecordSlow);
    if (Utils.isAvro14()) {
      Assert.assertEquals(testRecordFast.toString(), testRecordSlow.toString());
    } else {
      Assert.assertEquals(testRecordSlow, testRecordFast);
    }
  }

  @Test(groups = {"deserializationTest"})
  public void shouldReadGenericLikeSlow() throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
    oldSubRecord.put("oldSubField", "testValueOfSubField");
    oldSubRecord.put("fieldToBeRemoved", 33);
    oldRecord.put("oldSubRecord", oldSubRecord);

    // when
    GenericRecord testRecordSlow =
        decodeGenericSlow(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    GenericRecord testRecordFast =
        decodeGenericFast(DefaultsTestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));

    // then
    // map compare is not available in avro-1.4
    if (Utils.isAvro14()) {
      Assert.assertEquals(testRecordFast.toString(), testRecordSlow.toString());
    } else {
      Assert.assertEquals(testRecordSlow, testRecordFast);
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldAddFieldsInMiddleOfSchema(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = TestRecord.SCHEMA$;

    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    Schema enumSchema = Schema.createEnum("TestEnum", "", "", Arrays.asList("A", "B", "C", "D", "E"));

    GenericData.EnumSymbol testEnum = AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A");
    GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema());
    testFixed.bytes(new byte[]{0x01});
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);

    oldRecord.put("testInt", 1);
    oldRecord.put("testLong", 1L);
    oldRecord.put("testDouble", 1.0);
    oldRecord.put("testFloat", 1.0f);
    oldRecord.put("testBoolean", true);
    oldRecord.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    oldRecord.put("testString", "aaa");
    oldRecord.put("testFixed", testFixed);
    oldRecord.put("testEnum", testEnum);

    subRecord.put("subField", "abc");
    subRecord.put("anotherField", "ghi");

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);
    oldRecord.put("recordsArray", Collections.singletonList(subRecord));
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecord);
    oldRecord.put("recordsMap", recordsMap);

    oldRecord.put("testFixedArray", Collections.emptyList());
    oldRecord.put("testFixedUnionArray", Collections.emptyList());
    oldRecord.put("testEnumArray", Collections.emptyList());
    oldRecord.put("testEnumUnionArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    Schema newRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/defaultsTestSubrecord.avsc"));
    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer || Utils.isAvro14()) {
      record = decodeGenericFast(newRecordSchema, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      // There is a bug in Schema.applyAliases of avro-1.4, and the following invocation will trigger it.
      record = decodeGenericSlow(newRecordSchema, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    GenericData.Record newSubRecord =
        new GenericData.Record(newRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    newSubRecord.put("subField", new Utf8("abc"));
    newSubRecord.put("anotherField", new Utf8("ghi"));
    newSubRecord.put("newSubField", new Utf8("newSubFieldValue"));
    Map<Utf8, GenericData.Record> expectedRecordsMap = new HashMap<>();
    expectedRecordsMap.put(new Utf8("1"), newSubRecord);

    Assert.assertEquals("newSubFieldValue",
        ((GenericRecord) record.get("subRecordUnion")).get("newSubField").toString());
    Assert.assertEquals("newFieldValue", record.get("newField").toString());
    Assert.assertEquals(1, record.get("testInt"));
    Assert.assertEquals(1L, record.get("testLong"));
    Assert.assertEquals(1.0, record.get("testDouble"));
    Assert.assertEquals(1.0f, record.get("testFloat"));
    Assert.assertEquals(true, record.get("testBoolean"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.get("testBytes"));
    Assert.assertEquals(new Utf8("aaa"), record.get("testString"));
    Assert.assertEquals(testFixed, record.get("testFixed"));
    Assert.assertEquals(testEnum, record.get("testEnum"));
    Assert.assertEquals(newSubRecord, record.get("subRecordUnion"));

    Assert.assertTrue(Arrays.asList(newSubRecord).equals(record.get("recordsArray")));

    Assert.assertEquals(expectedRecordsMap, record.get("recordsMap"));
    Assert.assertTrue(Collections.emptyList().equals(record.get("testFixedArray")));
    Assert.assertTrue(Collections.emptyList().equals(record.get("testFixedUnionArray")));
    Assert.assertTrue(Collections.emptyList().equals(record.get("testEnumArray")));
    Assert.assertTrue(Collections.emptyList().equals(record.get("testEnumUnionArray")));
    Assert.assertTrue(Collections.emptyList().equals(record.get("recordsArrayMap")));
    Assert.assertEquals(Collections.emptyMap(), record.get("recordsMapArray"));
  }

  private GenericRecord newGenericSubRecord(String subField, String anotherField, String arrayEnumValue) {
    GenericRecord record = new GenericData.Record(DefaultsSubRecord.SCHEMA$);
    record.put("subField", subField);
    record.put("anotherField", anotherField);
    record.put("arrayField", Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(null, arrayEnumValue)));
    return record;
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeSpecificSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    org.apache.avro.io.DatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
    try {
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private GenericRecord decodeGenericSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    org.apache.avro.io.DatumReader<GenericData> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    try {
      return (GenericRecord) datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeSpecificFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastSpecificDeserializerGenerator(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    try {
      return deserializer.deserialize(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private GenericRecord decodeGenericFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    FastDeserializer<GenericRecord> deserializer =
        new FastGenericDeserializerGenerator(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    try {
      return deserializer.deserialize(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
