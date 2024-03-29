package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveBooleanList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveDoubleList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveFloatList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveIntList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveLongList;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeEnums;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeFixed;
import com.linkedin.avro.fastserde.generated.avro.FixedOfSize10;
import com.linkedin.avro.fastserde.generated.avro.JustSimpleEnum;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastGenericSerializerGeneratorTest {

  private File tempDir;
  private ClassLoader classLoader;

  @BeforeTest(groups = {"serializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastGenericSerializerGeneratorTest.class.getClassLoader());

    // In order to test the functionality of the record split we set an unusually low number
    FastGenericSerializerGenerator.setFieldsPerRecordSerializationMethod(2);
  }

  @Test(groups = {"serializationTest"})
  public void shouldWritePrimitives() {
    // given
    Schema recordSchema = createRecord(
        createField("testInt", Schema.create(Schema.Type.INT)),
        createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
        createUnionField("testFlippedIntUnion", Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)),
        createField("testString", Schema.create(Schema.Type.STRING)),
        createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING),
        createField("testLong", Schema.create(Schema.Type.LONG)),
        createPrimitiveUnionFieldSchema("testLongUnion", Schema.Type.LONG),
        createField("testDouble", Schema.create(Schema.Type.DOUBLE)),
        createPrimitiveUnionFieldSchema("testDoubleUnion", Schema.Type.DOUBLE),
        createField("testFloat", Schema.create(Schema.Type.FLOAT)),
        createPrimitiveUnionFieldSchema("testFloatUnion", Schema.Type.FLOAT),
        createField("testBoolean", Schema.create(Schema.Type.BOOLEAN)),
        createPrimitiveUnionFieldSchema("testBooleanUnion", Schema.Type.BOOLEAN),
        createField("testBytes", Schema.create(Schema.Type.BYTES)),
        createPrimitiveUnionFieldSchema("testBytesUnion", Schema.Type.BYTES));

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("testInt", 1);
    builder.put("testIntUnion", 1);
    builder.put("testFlippedIntUnion", null);
    builder.put("testString", "aaa");
    builder.put("testStringUnion", "aaa");
    builder.put("testLong", 1L);
    builder.put("testLongUnion", 1L);
    builder.put("testDouble", 1.0);
    builder.put("testDoubleUnion", 1.0);
    builder.put("testFloat", 1.0f);
    builder.put("testFloatUnion", 1.0f);
    builder.put("testBoolean", true);
    builder.put("testBooleanUnion", true);
    builder.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    builder.put("testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals(1, record.get("testInt"));
    Assert.assertEquals(1, record.get("testIntUnion"));
    Assert.assertNull(record.get("testFlippedIntUnion"));
    Assert.assertEquals("aaa", record.get("testString").toString());
    Assert.assertEquals("aaa", record.get("testStringUnion").toString());
    Assert.assertEquals(1L, record.get("testLong"));
    Assert.assertEquals(1L, record.get("testLongUnion"));
    Assert.assertEquals(1.0, record.get("testDouble"));
    Assert.assertEquals(1.0, record.get("testDoubleUnion"));
    Assert.assertEquals(1.0f, record.get("testFloat"));
    Assert.assertEquals(1.0f, record.get("testFloatUnion"));
    Assert.assertEquals(true, record.get("testBoolean"));
    Assert.assertEquals(true, record.get("testBooleanUnion"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.get("testBytes"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.get("testBytesUnion"));
  }

  public GenericData.Fixed newFixed(Schema fixedSchema, byte[] bytes) {
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema);
    fixed.bytes(bytes);
    return fixed;
  }

  @SuppressWarnings("unchecked")
  @Test(groups = {"serializationTest"})
  public void shouldWriteGenericRecordWithFixed() {
    // given
    Schema fixedSchema = createFixedSchema("testFixed", 2);
    Schema recordSchema = createRecord(
        createField("testFixed", fixedSchema),
        createUnionFieldWithNull("testFixedUnion", fixedSchema),
        createArrayFieldSchema("testFixedArray", fixedSchema),
        createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("testFixed", newFixed(fixedSchema, new byte[]{0x01, 0x02}));
    builder.put("testFixedUnion", newFixed(fixedSchema, new byte[]{0x03, 0x04}));
    builder.put("testFixedArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x05, 0x06})));
    builder.put("testFixedUnionArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x07, 0x08})));

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals(new byte[]{0x01, 0x02}, ((GenericData.Fixed) record.get("testFixed")).bytes());
    Assert.assertEquals(new byte[]{0x03, 0x04}, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x05, 0x06},
        ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x07, 0x08},
        ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSpecificRecordWithFixed() {
    // given
    final byte[] bytes1 = "2023-09-07".getBytes();
    final byte[] bytes2 = "2023-09-08".getBytes();
    final byte[] bytes3 = "2023-09-09".getBytes();

    Function<byte[], FixedOfSize10> fixedCreator = bytes -> {
      FixedOfSize10 fixedOfSize10 = new FixedOfSize10();
      fixedOfSize10.bytes(bytes);
      return fixedOfSize10;
    };

    Map<CharSequence, FixedOfSize10> mapOfFixed = new HashMap<>();
    mapOfFixed.put("day1", fixedCreator.apply(bytes1));
    mapOfFixed.put("day2", fixedCreator.apply(bytes2));

    FastSerdeFixed fastSerdeFixed = new FastSerdeFixed();
    setField(fastSerdeFixed, "fixedField", fixedCreator.apply(bytes1));
    setField(fastSerdeFixed, "arrayOfFixed", Lists.newArrayList(
            fixedCreator.apply(bytes1), fixedCreator.apply(bytes2), fixedCreator.apply(bytes3)));
    setField(fastSerdeFixed, "mapOfFixed", mapOfFixed);

    // when
    GenericRecord record = decodeRecord(fastSerdeFixed.getSchema(), dataAsBinaryDecoder(fastSerdeFixed));

    // then
    Assert.assertTrue(record.get("fixedField") instanceof GenericData.Fixed);
    Assert.assertEquals(((GenericData.Fixed) record.get("fixedField")).bytes(), bytes1);

    GenericData.Array<?> arrayOfFixed = (GenericData.Array<?>) record.get("arrayOfFixed");
    Assert.assertEquals(arrayOfFixed.size(), 3);
    Assert.assertTrue(arrayOfFixed.get(0) instanceof GenericData.Fixed);
    Assert.assertEquals(((GenericData.Fixed) arrayOfFixed.get(0)).bytes(), bytes1);
    Assert.assertEquals(((GenericData.Fixed) arrayOfFixed.get(1)).bytes(), bytes2);
    Assert.assertEquals(((GenericData.Fixed) arrayOfFixed.get(2)).bytes(), bytes3);

    @SuppressWarnings("unchecked")
    Map<CharSequence, GenericData.Fixed> deserializedMapOfFixed = (Map<CharSequence, GenericData.Fixed>) record.get("mapOfFixed");
    Assert.assertEquals(deserializedMapOfFixed.size(), 2);
    Assert.assertEquals(deserializedMapOfFixed.get(new Utf8("day1")).bytes(), bytes1);
    Assert.assertEquals(deserializedMapOfFixed.get(new Utf8("day2")).bytes(), bytes2);
  }

  @SuppressWarnings("unchecked")
  @Test(groups = {"serializationTest"})
  public void shouldWriteGenericRecordWithEnums() {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema = createRecord(
        createField("testEnum", enumSchema),
        createUnionFieldWithNull("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")); // GenericData.EnumSymbol("A"));
    builder.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")); //new GenericData.EnumSymbol("A"));
    builder.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")));//new GenericData.EnumSymbol("A")));
    builder.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"))); //new GenericData.EnumSymbol("A")));

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
    Assert.assertEquals("A", record.get("testEnumUnion").toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSpecificRecordWithEnums() {
    // given
    Map<CharSequence, JustSimpleEnum> mapOfEnums = new HashMap<>();
    mapOfEnums.put("due", JustSimpleEnum.E2);
    mapOfEnums.put("cinque", JustSimpleEnum.E5);

    FastSerdeEnums fastSerdeEnums = new FastSerdeEnums();
    setField(fastSerdeEnums, "enumField", JustSimpleEnum.E1);
    setField(fastSerdeEnums, "arrayOfEnums", Lists.newArrayList(JustSimpleEnum.E1, JustSimpleEnum.E3, JustSimpleEnum.E4));
    setField(fastSerdeEnums, "mapOfEnums", mapOfEnums);

    // when
    GenericRecord record = decodeRecord(fastSerdeEnums.getSchema(), dataAsBinaryDecoder(fastSerdeEnums));

    // then
    Assert.assertTrue(record.get("enumField") instanceof GenericData.EnumSymbol);
    Assert.assertEquals(record.get("enumField").toString(), "E1");

    GenericData.Array<?> arrayOfEnums = (GenericData.Array<?>) record.get("arrayOfEnums");
    Assert.assertEquals(arrayOfEnums.size(), 3);
    Assert.assertEquals(arrayOfEnums.get(0).toString(), JustSimpleEnum.E1.name());
    Assert.assertEquals(arrayOfEnums.get(1).toString(), JustSimpleEnum.E3.name());
    Assert.assertEquals(arrayOfEnums.get(2).toString(), JustSimpleEnum.E4.name());

    @SuppressWarnings("unchecked")
    Map<CharSequence, GenericData.EnumSymbol> deserializedMapOfEnums = (Map<CharSequence, GenericData.EnumSymbol>) record.get("mapOfEnums");
    Assert.assertEquals(deserializedMapOfEnums.size(), 2);
    Assert.assertEquals(deserializedMapOfEnums.get(new Utf8("due")).toString(), JustSimpleEnum.E2.toString());
    Assert.assertEquals(deserializedMapOfEnums.get(new Utf8("cinque")).toString(), JustSimpleEnum.E5.toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordField() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord(
        createUnionFieldWithNull("record", subRecordSchema),
        createField("record1", subRecordSchema),
        createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("record", subRecordBuilder);
    builder.put("record1", subRecordBuilder);
    builder.put("field", "abc");

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("abc", ((GenericRecord) record.get("record")).get("subField").toString());
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
    Assert.assertEquals("abc", ((GenericRecord) record.get("record1")).get("subField").toString());
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
    Assert.assertEquals("abc", record.get("field").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteRightUnionIndex() {
    // Create two record schemas
    Schema recordSchema1 = createRecord("record1", createField("record1_field1", Schema.create(Schema.Type.STRING)));
    Schema recordSchema2 = createRecord("record2", createField("record2_field1", Schema.create(Schema.Type.STRING)));
    Schema unionSchema = createUnionSchema(recordSchema1, recordSchema2);
    Schema recordWrapperSchema = createRecord(createField("union_field", unionSchema));

    GenericData.Record objectOfRecordSchema2 = new GenericData.Record(recordSchema2);
    objectOfRecordSchema2.put("record2_field1", "abc");
    GenericData.Record wrapperObject = new GenericData.Record(recordWrapperSchema);
    wrapperObject.put("union_field", objectOfRecordSchema2);

    GenericRecord record = decodeRecord(recordWrapperSchema, dataAsBinaryDecoder(wrapperObject));

    Object unionField = record.get("union_field");
    Assert.assertTrue(unionField instanceof GenericData.Record);
    GenericData.Record unionRecord = (GenericData.Record)unionField;
    Assert.assertEquals(unionRecord.getSchema().getName(), "record2");
  }

  @SuppressWarnings("unchecked")
  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordCollectionsField() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord(
        createArrayFieldSchema("recordsArray", subRecordSchema),
        createMapFieldSchema("recordsMap", subRecordSchema),
        createUnionFieldWithNull("recordsArrayUnion", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionFieldWithNull("recordsMapUnion", Schema.createMap(createUnionSchema(subRecordSchema))));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    List<GenericData.Record> recordsArray = new ArrayList<>();
    recordsArray.add(subRecordBuilder);
    builder.put("recordsArray", recordsArray);
    builder.put("recordsArrayUnion", recordsArray);
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    builder.put("recordsMap", recordsMap);
    builder.put("recordsMapUnion", recordsMap);

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("abc",
        ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField").toString());
    Assert.assertEquals("abc",
        ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField").toString());
    Assert.assertEquals("abc",
        ((Map<CharSequence, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField").toString());
    Assert.assertEquals("abc", ((Map<CharSequence, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1"))
        .get("subField")
        .toString());
  }

  @SuppressWarnings("unchecked")
  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordComplexCollectionsField() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord(
        createArrayFieldSchema("recordsArrayMap", Schema.createMap(createUnionSchema(subRecordSchema))),
        createMapFieldSchema("recordsMapArray", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionFieldWithNull("recordsArrayMapUnion",
            Schema.createArray(Schema.createMap(createUnionSchema(subRecordSchema)))),
        createUnionFieldWithNull("recordsMapArrayUnion",
            Schema.createMap(Schema.createArray(createUnionSchema(subRecordSchema)))));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    List<Map<String, GenericRecord>> recordsArrayMap = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new HashMap<>();
    recordMap.put("1", subRecordBuilder);
    recordsArrayMap.add(recordMap);

    builder.put("recordsArrayMap", recordsArrayMap);
    builder.put("recordsArrayMapUnion", recordsArrayMap);

    Map<String, List<GenericRecord>> recordsMapArray = new HashMap<>();
    List<GenericRecord> recordList = new ArrayList<>();
    recordList.add(subRecordBuilder);
    recordsMapArray.put("1", recordList);

    builder.put("recordsMapArray", recordsMapArray);
    builder.put("recordsMapArrayUnion", recordsMapArray);

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("abc", ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMap")).get(0)
        .get(new Utf8("1"))
        .get("subField")
        .toString());
    Assert.assertEquals("abc", ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArray")).get(new Utf8("1"))
        .get(0)
        .get("subField")
        .toString());
    Assert.assertEquals("abc", ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0)
        .get(new Utf8("1"))
        .get("subField")
        .toString());
    Assert.assertEquals("abc", ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get(new Utf8("1"))
        .get(0)
        .get("subField")
        .toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMultipleChoiceUnion() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord(
        createUnionFieldWithNull("union", subRecordSchema, Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("union", subRecordBuilder);

    // when
    GenericRecord record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("abc", ((GenericData.Record) record.get("union")).get("subField").toString());

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", "abc");

    // when
    record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals("abc", record.get("union").toString());

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", 1);

    // when
    record = decodeRecord(recordSchema, dataAsBinaryDecoder(builder));

    // then
    Assert.assertEquals(1, record.get("union"));
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfRecords() {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema arrayRecordSchema = Schema.createArray(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    GenericData.Array<GenericData.Record> recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    GenericData.Array<GenericRecord> array = decodeRecord(arrayRecordSchema, dataAsBinaryDecoder(recordsArray));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", array.get(0).get("field").toString());
    Assert.assertEquals("abc", array.get(1).get("field").toString());

    // given

    arrayRecordSchema = Schema.createArray(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    array = decodeRecord(arrayRecordSchema, dataAsBinaryDecoder(recordsArray));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", array.get(0).get("field").toString());
    Assert.assertEquals("abc", array.get(1).get("field").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMapOfRecords() {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema mapRecordSchema = Schema.createMap(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    Map<Utf8, GenericRecord> map = decodeRecord(mapRecordSchema, dataAsBinaryDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
    Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());

    // given
    mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    map = decodeRecord(mapRecordSchema, dataAsBinaryDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
    Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfBoolean() {
    // given
    AtomicBoolean primitiveApiCalled = new AtomicBoolean(false);
    List<Boolean> data = new ColdPrimitiveBooleanList(2) {
      @Override
      public boolean getPrimitive(int index) {
        primitiveApiCalled.set(true);
        return get(index);
      }
    };
    data.add(true);
    data.add(false);

    // then
    shouldWriteArrayOfPrimitives(Schema.Type.BOOLEAN, data);
    Assert.assertTrue(primitiveApiCalled.get());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfDouble() {
    // given
    AtomicBoolean primitiveApiCalled = new AtomicBoolean(false);
    List<Double> data = new ColdPrimitiveDoubleList(2) {
      @Override
      public double getPrimitive(int index) {
        primitiveApiCalled.set(true);
        return get(index);
      }
    };
    data.add(1.0D);
    data.add(2.0D);

    // then
    shouldWriteArrayOfPrimitives(Schema.Type.DOUBLE, data);
    Assert.assertTrue(primitiveApiCalled.get());
  }

  @Test(groups = {"serializationTest"})
  public void shouldPassThroughByteBufferForArrayOfFloats() {
    String arrayOfFloatFieldName = "array_of_float";
    Schema recordSchema = createRecord("TestArrayOfFloats", new Schema.Field(arrayOfFloatFieldName, Schema.createArray(Schema.create(Schema.Type.FLOAT)), null, null));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put(arrayOfFloatFieldName, Arrays.asList(1.0f, 2.0f, 3.0f));

    /**
     * Deserialize it first by fast deserializer to check whether {@link BufferBackedPrimitiveFloatList} is being used or not.
      */
    GenericRecord decodedRecord = decodeRecordFast(recordSchema, dataAsBinaryDecoder(record));
    Assert.assertTrue(decodedRecord.get(arrayOfFloatFieldName) instanceof BufferBackedPrimitiveFloatList);

    class TestBufferBackedPrimitiveFloatList extends BufferBackedPrimitiveFloatList {

      boolean writeFloatsCalled = false;
      boolean writeFloatsByBackedBytesCalled = false;
      public TestBufferBackedPrimitiveFloatList(BufferBackedPrimitiveFloatList floatList) {
        super(0);
        floatList.copyInternalState(this);
      }

      public void resetFlag() {
        this.writeFloatsCalled = false;
        this.writeFloatsByBackedBytesCalled = false;
      }

      @Override
      public void writeFloats(Encoder encoder) throws IOException {
        writeFloatsCalled = true;
        super.writeFloats(encoder);
      }

      @Override
      protected void writeFloatsByBackedBytes(Encoder encoder) throws IOException {
        writeFloatsByBackedBytesCalled = true;
        super.writeFloatsByBackedBytes(encoder);
      }
    }

    TestBufferBackedPrimitiveFloatList floatListWithHook = new TestBufferBackedPrimitiveFloatList((BufferBackedPrimitiveFloatList)decodedRecord.get(arrayOfFloatFieldName));

    // Replace the record field by the object with hook function
    decodedRecord.put(arrayOfFloatFieldName, floatListWithHook);

    // Serialize it with fast serializer
    Decoder anotherDecoder = dataAsBinaryDecoder(decodedRecord, recordSchema);

    Assert.assertTrue(floatListWithHook.writeFloatsCalled);
    Assert.assertTrue(floatListWithHook.writeFloatsByBackedBytesCalled);

    // Deserialize it by vanilla Avro to verify data
    GenericRecord decodedRecord1 = decodeRecord(recordSchema, anotherDecoder);
    List<Float> decodedFloatList1 = (List<Float>)decodedRecord1.get(arrayOfFloatFieldName);
    Assert.assertEquals(decodedFloatList1.get(0), 1.0f);
    Assert.assertEquals(decodedFloatList1.get(1), 2.0f);
    Assert.assertEquals(decodedFloatList1.get(2), 3.0f);

    /**
     * Change the elements of {@link BufferBackedPrimitiveFloatList}, then pass-through bytes won't be used in fast serializer.
     */
    floatListWithHook.set(0, 10.0f);
    floatListWithHook.resetFlag();
    // Serialize it again with fast serializer
    GenericRecord decodedRecord2 = decodeRecord(recordSchema, dataAsBinaryDecoder(decodedRecord, recordSchema));

    Assert.assertTrue(floatListWithHook.writeFloatsCalled);
    Assert.assertFalse(floatListWithHook.writeFloatsByBackedBytesCalled);
    List<Float> decodedFloatList2 = (List<Float>)decodedRecord2.get(arrayOfFloatFieldName);
    Assert.assertEquals(decodedFloatList2.get(0), 10.0f);
    Assert.assertEquals(decodedFloatList2.get(1), 2.0f);
    Assert.assertEquals(decodedFloatList2.get(2), 3.0f);

  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfFloats() {
    // given
    AtomicBoolean primitiveApiCalled = new AtomicBoolean(false);
    List<Float> data = new ColdPrimitiveFloatList(2) {
      @Override
      public float getPrimitive(int index) {
        primitiveApiCalled.set(true);
        return get(index);
      }
    };
    data.add(1.0F);
    data.add(2.0F);

    // then
    shouldWriteArrayOfPrimitives(Schema.Type.FLOAT, data);
    Assert.assertTrue(primitiveApiCalled.get());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfInts() {
    // given
    AtomicBoolean primitiveApiCalled = new AtomicBoolean(false);
    List<Integer> data = new ColdPrimitiveIntList(2) {
      @Override
      public int getPrimitive(int index) {
        primitiveApiCalled.set(true);
        return get(index);
      }
    };
    data.add(1);
    data.add(2);

    // then
    shouldWriteArrayOfPrimitives(Schema.Type.INT, data);
    Assert.assertTrue(primitiveApiCalled.get());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfLongs() {
    // given
    AtomicBoolean primitiveApiCalled = new AtomicBoolean(false);
    List<Long> data = new ColdPrimitiveLongList(2) {
      @Override
      public long getPrimitive(int index) {
        primitiveApiCalled.set(true);
        return get(index);
      }
    };
    data.add(1L);
    data.add(2L);

    // then
    shouldWriteArrayOfPrimitives(Schema.Type.LONG, data);
    Assert.assertTrue(primitiveApiCalled.get());
  }

  @Test(groups = {"serializationTest"})
  public void shouldSerializeLargeRecord() {
    int fieldCnt = 1000;
    String fieldNamePrefix = "field_";
    Schema.Field[] fields = new Schema.Field[fieldCnt];
    for (int i = 0; i < fieldCnt; ++i) {
      fields[i] = createPrimitiveFieldSchema(fieldNamePrefix + i, Schema.Type.INT);
    }
    Schema recordSchema = createRecord(fields);
    GenericRecord record = new GenericData.Record(recordSchema);
    for (int i = 0; i < fieldCnt; ++i) {
      record.put(fieldNamePrefix + i, new Integer(i));
    }

    GenericRecord decodedRecord = decodeRecord(recordSchema, dataAsBinaryDecoder(record, recordSchema));
    for (int i = 0; i < fieldCnt; ++i) {
      Assert.assertEquals(decodedRecord.get(fieldNamePrefix + i), new Integer(i));
    }
  }

  private <E> void shouldWriteArrayOfPrimitives(Schema.Type elementType, List<E> data) {
    // given
    Schema elementSchema = Schema.create(elementType);
    Schema arraySchema = Schema.createArray(elementSchema);

    // Serialization should work on various types of lists
    GenericData.Array<E> vanillaAvroList = new GenericData.Array<>(0, arraySchema);
    ArrayList<E> javaList = new ArrayList<>(0);
    for (E element: data) {
      vanillaAvroList.add(element);
      javaList.add(element);
    }

    // when
    List<E> resultFromAvroList = decodeRecord(arraySchema, dataAsBinaryDecoder(vanillaAvroList));
    List<E> resultFromJavaList = decodeRecord(arraySchema, dataAsBinaryDecoder(javaList, arraySchema));
    List<E> resultFromPrimitiveList = decodeRecord(arraySchema, dataAsBinaryDecoder(data, arraySchema));

    // then
    Assert.assertEquals(resultFromAvroList.size(), data.size());
    Assert.assertEquals(resultFromJavaList.size(), data.size());
    Assert.assertEquals(resultFromPrimitiveList.size(), data.size());
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(resultFromAvroList.get(i), data.get(i));
      Assert.assertEquals(resultFromJavaList.get(i), data.get(i));
      Assert.assertEquals(resultFromPrimitiveList.get(i), data.get(i));
    }
  }

  public <T extends GenericContainer> Decoder dataAsBinaryDecoder(T data) {
    return dataAsBinaryDecoder(data, data.getSchema());
  }

  public <T> Decoder dataAsBinaryDecoder(T data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null); //new BinaryEncoder(baos);

    try {
      FastGenericSerializerGenerator<T> fastGenericSerializerGenerator =
          new FastGenericSerializerGenerator<>(schema, tempDir, classLoader, null, null);
      FastSerializer<T> fastSerializer = fastGenericSerializerGenerator.generateSerializer();
      fastSerializer.serialize(data, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
  }

  public <T> T decodeRecord(Schema schema, Decoder decoder) {
    GenericDatumReader<T> datumReader = new GenericDatumReader<>(schema);
    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T decodeRecordFast(Schema schema, Decoder decoder) {
    try {
      FastGenericDeserializerGenerator<T> fastGenericDeserializerGenerator = new FastGenericDeserializerGenerator<>(schema, schema, tempDir, classLoader, null, null);
      FastDeserializer<T> fastDeserializer = fastGenericDeserializerGenerator.generateDeserializer();
      return fastDeserializer.deserialize(decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
