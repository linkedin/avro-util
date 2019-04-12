package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastGenericDeserializerGeneratorTest {

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
        FastGenericDeserializerGeneratorTest.class.getClassLoader());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadPrimitives(Boolean whetherUseFastDeserializer) {
    // given
    Schema recordSchema = createRecord("testRecord", createField("testInt", Schema.create(Schema.Type.INT)),
        createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
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

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("testInt", 1);
    record.put("testIntUnion", 1);
    record.put("testString", "aaa");
    record.put("testStringUnion", "aaa");
    record.put("testLong", 1l);
    record.put("testLongUnion", 1l);
    record.put("testDouble", 1.0);
    record.put("testDoubleUnion", 1.0);
    record.put("testFloat", 1.0f);
    record.put("testFloatUnion", 1.0f);
    record.put("testBoolean", true);
    record.put("testBooleanUnion", true);
    record.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    record.put("testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

    // when
    GenericRecord decodedRecord = null;
    if (whetherUseFastDeserializer) {
      decodedRecord = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(record));
    } else {
      decodedRecord = decodeRecordSlow(recordSchema, recordSchema, genericDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(1, decodedRecord.get("testInt"));
    Assert.assertEquals(1, decodedRecord.get("testIntUnion"));
    Assert.assertEquals(new Utf8("aaa"), decodedRecord.get("testString"));
    Assert.assertEquals(new Utf8("aaa"), decodedRecord.get("testStringUnion"));
    Assert.assertEquals(1l, decodedRecord.get("testLong"));
    Assert.assertEquals(1l, decodedRecord.get("testLongUnion"));
    Assert.assertEquals(1.0, decodedRecord.get("testDouble"));
    Assert.assertEquals(1.0, decodedRecord.get("testDoubleUnion"));
    Assert.assertEquals(1.0f, decodedRecord.get("testFloat"));
    Assert.assertEquals(1.0f, decodedRecord.get("testFloatUnion"));
    Assert.assertEquals(true, decodedRecord.get("testBoolean"));
    Assert.assertEquals(true, decodedRecord.get("testBooleanUnion"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), decodedRecord.get("testBytes"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), decodedRecord.get("testBytesUnion"));
  }

  public GenericData.Fixed newFixed(Schema fixedSchema, byte[] bytes) {
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema);
    fixed.bytes(bytes);
    return fixed;
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadFixed(Boolean whetherUseFastDeserializer) {
    // given
    Schema fixedSchema = createFixedSchema("testFixed", 2);
    Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
        createUnionField("testFixedUnion", fixedSchema), createArrayFieldSchema("testFixedArray", fixedSchema),
        createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testFixed", newFixed(fixedSchema, new byte[]{0x01, 0x02}));
    originalRecord.put("testFixedUnion", newFixed(fixedSchema, new byte[]{0x03, 0x04}));
    originalRecord.put("testFixedArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x05, 0x06})));
    originalRecord.put("testFixedUnionArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x07, 0x08})));

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));
    } else {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));
    }

    // then
    Assert.assertEquals(new byte[]{0x01, 0x02}, ((GenericData.Fixed) record.get("testFixed")).bytes());
    Assert.assertEquals(new byte[]{0x03, 0x04}, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x05, 0x06},
        ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x07, 0x08},
        ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadEnum(Boolean whetherUseFastDeserializer) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema =
        createRecord("testRecord", createField("testEnum", enumSchema), createUnionField("testEnumUnion", enumSchema),
            createArrayFieldSchema("testEnumArray", enumSchema),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")));//new GenericData.EnumSymbol("A")));
    originalRecord.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")));//new GenericData.EnumSymbol("A")));

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));
    } else {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));
    }

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
    Assert.assertEquals("A", record.get("testEnumUnion").toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadPermutatedEnum(Boolean whetherUseFastDeserializer) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C", "D", "E"});
    Schema recordSchema =
        createRecord("testRecord", createField("testEnum", enumSchema), createUnionField("testEnumUnion", enumSchema),
            createArrayFieldSchema("testEnumArray", enumSchema),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "B"));//new GenericData.EnumSymbol("B"));
    originalRecord.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C")));//new GenericData.EnumSymbol("C")));
    originalRecord.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "D")));//new GenericData.EnumSymbol("D")));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"B", "A", "D", "E", "C"});
    Schema recordSchema1 =
        createRecord("testRecord", createField("testEnum", enumSchema1), createUnionField("testEnumUnion", enumSchema1),
            createArrayFieldSchema("testEnumArray", enumSchema1),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));
    } else {
      record = decodeRecordFast(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));
    }

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
    Assert.assertEquals("B", record.get("testEnumUnion").toString());
    Assert.assertEquals("C", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("D", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
  }

  @Test(expectedExceptions = FastDeserializerGeneratorException.class, groups = {"deserializationTest"})
  public void shouldNotReadStrippedEnum() {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C"});
    Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(null, "C"));//new GenericData.EnumSymbol("C"));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1));

    // when
    GenericRecord record = decodeRecordFast(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordField(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema =
        createRecord("test", createUnionField("record", subRecordSchema), createField("record1", subRecordSchema),
            createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    GenericRecord subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericRecord builder = new GenericData.Record(recordSchema);
    builder.put("record", subRecordBuilder);
    builder.put("record1", subRecordBuilder);
    builder.put("field", "abc");

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordSlow(recordSchema, recordSchema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("record")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("record1")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), record.get("field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordCollectionsField(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord("test", createArrayFieldSchema("recordsArray", subRecordSchema),
        createMapFieldSchema("recordsMap", subRecordSchema),
        createUnionField("recordsArrayUnion", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionField("recordsMapUnion", Schema.createMap(createUnionSchema(subRecordSchema))));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericRecord builder = new GenericData.Record(recordSchema);
    List<GenericData.Record> recordsArray = new ArrayList<>();
    recordsArray.add(subRecordBuilder);
    builder.put("recordsArray", recordsArray);
    builder.put("recordsArrayUnion", recordsArray);
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    builder.put("recordsMap", recordsMap);
    builder.put("recordsMapUnion", recordsMap);

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"),
        ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1")).get("subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordComplexCollectionsField(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord("test",
        createArrayFieldSchema("recordsArrayMap", Schema.createMap(createUnionSchema(subRecordSchema))),
        createMapFieldSchema("recordsMapArray", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionField("recordsArrayMapUnion",
            Schema.createArray(Schema.createMap(createUnionSchema(subRecordSchema)))),
        createUnionField("recordsMapArrayUnion",
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
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordSlow(recordSchema, recordSchema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"),
        ((List<Map<String, GenericRecord>>) record.get("recordsArrayMap")).get(0).get(new Utf8("1")).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, List<GenericRecord>>) record.get("recordsMapArray")).get(new Utf8("1")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"), ((List<Map<String, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0)
        .get(new Utf8("1"))
        .get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get(new Utf8("1"))
            .get(0)
            .get("subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadAliasedField(Boolean whetherUseFastDeserializer) {
    // given
    Schema record1Schema = createRecord("test", createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING));
    Schema record2Schema = createRecord("test", createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
        addAliases(createPrimitiveUnionFieldSchema("testStringUnionAlias", Schema.Type.STRING), "testStringUnion"));

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("testString", "abc");
    builder.put("testStringUnion", "def");

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.get("testString"));
    // Alias is not well supported in avro-1.4
    if (!Utils.isAvro14()) {
      Assert.assertEquals(new Utf8("def"), record.get("testStringUnionAlias"));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldSkipRemovedField(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecord1Schema =
        createRecord("subRecord", createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record1Schema = createRecord("test", createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionField("subRecord", subRecord1Schema), createMapFieldSchema("subRecordMap", subRecord1Schema),
        createArrayFieldSchema("subRecordArray", subRecord1Schema));
    Schema subRecord2Schema =
        createRecord("subRecord", createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record2Schema = createRecord("test", createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionField("subRecord", subRecord2Schema), createMapFieldSchema("subRecordMap", subRecord2Schema),
        createArrayFieldSchema("subRecordArray", subRecord2Schema));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("testNotRemoved", "abc");
    subRecordBuilder.put("testRemoved", "def");
    subRecordBuilder.put("testNotRemoved2", "ghi");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("testNotRemoved", "abc");
    builder.put("testRemoved", "def");
    builder.put("testNotRemoved2", "ghi");
    builder.put("subRecord", subRecordBuilder);
    builder.put("subRecordArray", Arrays.asList(subRecordBuilder));

    Map<String, GenericRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    builder.put("subRecordMap", recordsMap);

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.get("testNotRemoved"));
    Assert.assertNull(record.get("testRemoved"));
    Assert.assertEquals(new Utf8("ghi"), record.get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"), ((GenericRecord) record.get("subRecord")).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((List<GenericRecord>) record.get("subRecordArray")).get(0).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((Map<String, GenericRecord>) record.get("subRecordMap")).get(new Utf8("1")).get("testNotRemoved2"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldSkipRemovedRecord(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecord1Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord2Schema = createRecord("subRecord2", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));

    Schema record1Schema =
        createRecord("test", createField("subRecord1", subRecord1Schema), createField("subRecord2", subRecord2Schema),
            createUnionField("subRecord3", subRecord2Schema), createField("subRecord4", subRecord1Schema));

    Schema record2Schema =
        createRecord("test", createField("subRecord1", subRecord1Schema), createField("subRecord4", subRecord1Schema));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("test1", "abc");
    subRecordBuilder.put("test2", "def");

    GenericData.Record subRecordBuilder2 = new GenericData.Record(subRecord2Schema);
    subRecordBuilder2.put("test1", "ghi");
    subRecordBuilder2.put("test2", "jkl");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("subRecord1", subRecordBuilder);
    builder.put("subRecord2", subRecordBuilder2);
    builder.put("subRecord3", subRecordBuilder2);
    builder.put("subRecord4", subRecordBuilder);

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord1")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord1")).get("test2"));
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord4")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord4")).get("test2"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldSkipRemovedNestedRecord(Boolean whetherUseFastDeserializer) {
    // given
    Schema subSubRecordSchema = createRecord("subSubRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord1Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createField("test2", subSubRecordSchema), createUnionField("test3", subSubRecordSchema),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));
    Schema subRecord2Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));

    Schema record1Schema = createRecord("test", createField("subRecord", subRecord1Schema));

    Schema record2Schema = createRecord("test", createField("subRecord", subRecord2Schema));

    GenericData.Record subSubRecordBuilder = new GenericData.Record(subSubRecordSchema);
    subSubRecordBuilder.put("test1", "abc");
    subSubRecordBuilder.put("test2", "def");

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("test1", "abc");
    subRecordBuilder.put("test2", subSubRecordBuilder);
    subRecordBuilder.put("test3", subSubRecordBuilder);
    subRecordBuilder.put("test4", "def");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("subRecord", subRecordBuilder);

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(record1Schema, record2Schema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordSlow(record2Schema, record1Schema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord")).get("test4"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMultipleChoiceUnion(Boolean whetherUseFastDeserializer) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord("test",
        createUnionField("union", subRecordSchema, Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("union", subRecordBuilder);

    // when
    GenericRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordSlow(recordSchema, recordSchema, genericDataAsDecoder(builder));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericData.Record) record.get("union")).get("subField"));

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", "abc");

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));
    } else {
      record = decodeRecordSlow(recordSchema, recordSchema, genericDataAsDecoder(builder));
    }
    // then
    Assert.assertEquals(new Utf8("abc"), record.get("union"));

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", 1);

    // when
    record = decodeRecordFast(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(1, record.get("union"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadArrayOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema arrayRecordSchema = Schema.createArray(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    GenericData.Array<GenericData.Record> recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    GenericData.Array<GenericRecord> array = null;
    if (whetherUseFastDeserializer) {
      array = decodeRecordFast(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));
    } else {
      array = decodeRecordSlow(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));
    }

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).get("field"));
    Assert.assertEquals(new Utf8("abc"), array.get(1).get("field"));

    // given

    arrayRecordSchema = Schema.createArray(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    if (whetherUseFastDeserializer) {
      array = decodeRecordFast(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));
    } else {
      array = decodeRecordSlow(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));
    }
    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).get("field"));
    Assert.assertEquals(new Utf8("abc"), array.get(1).get("field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMapOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema mapRecordSchema = Schema.createMap(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    Map<String, GenericRecord> map = null;
    if (whetherUseFastDeserializer) {
      map = decodeRecordFast(mapRecordSchema, mapRecordSchema,
          FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));
    } else {
      map = decodeRecordSlow(mapRecordSchema, mapRecordSchema,
          FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));
    }

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).get("field"));
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).get("field"));

    // given
    mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    map = decodeRecordFast(mapRecordSchema, mapRecordSchema,
        FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).get("field"));
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).get("field"));
  }

  public <T> T decodeRecordFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastGenericDeserializerGenerator<T>(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    try {
      return deserializer.deserialize(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeRecordSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    org.apache.avro.io.DatumReader<GenericData> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    try {
      return (T) datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
