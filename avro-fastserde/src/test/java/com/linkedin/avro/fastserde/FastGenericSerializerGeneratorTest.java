package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.File;
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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastGenericSerializerGeneratorTest {

  private File tempDir;
  private ClassLoader classLoader;

  @BeforeTest(groups = {"serializationTest"})
  public void prepare() throws Exception {
    Path tempPath = Files.createTempDirectory("generated");
    tempDir = tempPath.toFile();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastGenericSerializerGeneratorTest.class.getClassLoader());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWritePrimitives() {
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

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("testInt", 1);
    builder.put("testIntUnion", 1);
    builder.put("testString", "aaa");
    builder.put("testStringUnion", "aaa");
    builder.put("testLong", 1l);
    builder.put("testLongUnion", 1l);
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
    Assert.assertEquals("aaa", record.get("testString").toString());
    Assert.assertEquals("aaa", record.get("testStringUnion").toString());
    Assert.assertEquals(1l, record.get("testLong"));
    Assert.assertEquals(1l, record.get("testLongUnion"));
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

  @Test(groups = {"serializationTest"})
  public void shouldWriteFixed() {
    // given
    Schema fixedSchema = createFixedSchema("testFixed", 2);
    Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
        createUnionField("testFixedUnion", fixedSchema), createArrayFieldSchema("testFixedArray", fixedSchema),
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
  public void shouldWriteEnum() {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema =
        createRecord("testRecord", createField("testEnum", enumSchema), createUnionField("testEnumUnion", enumSchema),
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
  public void shouldWriteSubRecordField() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema =
        createRecord("test", createUnionField("record", subRecordSchema), createField("record1", subRecordSchema),
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
  public void shouldWriteSubRecordCollectionsField() {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord("test", createArrayFieldSchema("recordsArray", subRecordSchema),
        createMapFieldSchema("recordsMap", subRecordSchema),
        createUnionField("recordsArrayUnion", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionField("recordsMapUnion", Schema.createMap(createUnionSchema(subRecordSchema))));

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
        ((Map<String, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField").toString());
    Assert.assertEquals("abc", ((Map<String, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1"))
        .get("subField")
        .toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordComplexCollectionsField() {
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

    Schema recordSchema = createRecord("test",
        createUnionField("union", subRecordSchema, Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)));

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

  public <T extends GenericContainer> Decoder dataAsBinaryDecoder(T data) {
    return dataAsBinaryDecoder(data, data.getSchema());
  }

  public <T> Decoder dataAsBinaryDecoder(T data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBufferedBinaryEncoder(baos); //new BinaryEncoder(baos);

    try {
      FastGenericSerializerGenerator<T> fastGenericSerializerGenerator =
          new FastGenericSerializerGenerator<>(schema, tempDir, classLoader, null);
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
}
