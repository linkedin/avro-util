package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastDeserializerGeneratorForReuseTest {
  private static Schema COMPLICATE_SCHEMA = Schema.parse(
      "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"testComplicateRecord\",\n" + "  \"fields\": [\n"
          + "   {\"name\": \"stringField\", \"type\": \"string\"},\n"
          + "   {\"name\": \"bytesField\", \"type\": \"bytes\"},\n"
          + "   {\"name\": \"intField\", \"type\": \"int\"},\n"
          + "   {\"name\": \"BigIntegerField\", \"type\": {\"type\": \"string\", \"java-class\": \"java.math.BigInteger\"}},\n"
          + "   {\"name\": \"fixedField\", \"type\": {\"type\": \"fixed\", \"name\": \"SimpleFixed\", \"size\": 16}},\n"
          + "   {\"name\": \"mapOfFixedField\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"fixed\", \"name\": \"SimpleFixedInMap\", \"size\": 16}} },\n"
          + "   {\"name\": \"mapOfIntegerField\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"string\", \"java-class\": \"java.math.BigInteger\"}}},\n"
          + "   {\"name\": \"mapField\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
          + "   {\"name\": \"arrayField\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
          + "   {\"name\": \"floatArrayField\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
          + "   {\"name\": \"nestedRecord\", \"type\": {\n" + "  \"type\": \"record\",\n"
          + "  \"name\": \"testNestedRecord\",\n" + "  \"fields\": [\n"
          + "   {\"name\": \"stringField\", \"type\": \"string\"},\n"
          + "   {\"name\": \"bytesField\", \"type\": \"bytes\"},\n"
          + "   {\"name\": \"intField\", \"type\": \"int\"},\n"
          + "   {\"name\": \"BigIntegerField\", \"type\": {\"type\": \"string\", \"java-class\": \"java.math.BigInteger\"}},\n"
          + "   {\"name\": \"fixedField\", \"type\": {\"type\": \"fixed\", \"name\": \"NestedSimpleFixed\", \"size\": 16}},\n"
          + "   {\"name\": \"mapOfFixedField\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"fixed\", \"name\": \"NestedSimpleFixedInMap\", \"size\": 16}} },\n"
          + "   {\"name\": \"mapOfIntegerField\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"string\", \"java-class\": \"java.math.BigInteger\"}}},\n"
          + "   {\"name\": \"mapField\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
          + "   {\"name\": \"arrayField\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
          + "   {\"name\": \"floatArrayField\", \"type\": {\"type\": \"array\", \"items\": \"float\"}}\n" + "  ]\n"
          + "  }\n" + "  }\n" + "  ]\n" + "}\n");
  private static Schema SCHEMA_FIXED = Schema.parse("{\"type\": \"fixed\", \"name\": \"SimpleFixed\", \"size\": 16}");
  private static Schema SCHEMA_NESTED_RECORD = Schema.parse(
      "{\n" + "      \"type\" : \"record\",\n" + "      \"name\" : \"testNestedRecord\",\n" + "      \"fields\" : [ {\n"
          + "        \"name\" : \"stringField\",\n" + "        \"type\" : \"string\"\n" + "      }, {\n"
          + "        \"name\" : \"bytesField\",\n" + "        \"type\" : \"bytes\"\n" + "      }, {\n"
          + "        \"name\" : \"intField\",\n" + "        \"type\" : \"int\"\n" + "      }, {\n"
          + "        \"name\" : \"BigIntegerField\",\n" + "        \"type\" : {\n"
          + "          \"type\" : \"string\",\n" + "          \"java-class\" : \"java.math.BigInteger\"\n"
          + "        }\n" + "      }, {\n" + "        \"name\" : \"fixedField\",\n" + "        \"type\" : {\n"
          + "          \"type\" : \"fixed\",\n" + "          \"name\" : \"NestedSimpleFixed\",\n"
          + "          \"size\" : 16\n" + "        }\n" + "      }, {\n" + "        \"name\" : \"mapOfFixedField\",\n"
          + "        \"type\" : {\n" + "          \"type\" : \"map\",\n" + "          \"values\" : {\n"
          + "            \"type\" : \"fixed\",\n" + "            \"name\" : \"NestedSimpleFixedInMap\",\n"
          + "            \"size\" : 16\n" + "          }\n" + "        }\n" + "      }, {\n"
          + "        \"name\" : \"mapOfIntegerField\",\n" + "        \"type\" : {\n" + "          \"type\" : \"map\",\n"
          + "          \"values\" : {\n" + "            \"type\" : \"string\",\n"
          + "            \"java-class\" : \"java.math.BigInteger\"\n" + "          }\n" + "        }\n" + "      }, {\n"
          + "        \"name\" : \"mapField\",\n" + "        \"type\" : {\n" + "          \"type\" : \"map\",\n"
          + "          \"values\" : \"string\"\n" + "        }\n" + "      }, {\n"
          + "        \"name\" : \"arrayField\",\n" + "        \"type\" : {\n" + "          \"type\" : \"array\",\n"
          + "          \"items\" : \"string\"\n" + "        }\n" + "      }, {\n"
          + "        \"name\" : \"floatArrayField\",\n" + "        \"type\" : {\n" + "          \"type\" : \"array\",\n"
          + "          \"items\" : \"float\"\n" + "        }\n" + "      } ]\n" + "    }");

  public static GenericRecord newComplicateRecord(char suffix) {
    GenericRecord record = new GenericData.Record(COMPLICATE_SCHEMA);
    record.put("stringField", "test_string" + suffix);
    record.put("bytesField", ByteBuffer.wrap(("test_bytes" + suffix).getBytes()));
    record.put("intField", 100);
    record.put("BigIntegerField", "123");
    GenericData.Fixed fixed = new GenericData.Fixed(SCHEMA_FIXED);
    fixed.bytes(("aaaaaaaaaaaaaaa" + suffix).getBytes());
    record.put("fixedField", fixed);
    Map<String, GenericData.Fixed> fixedMap = new HashMap<>();
    fixedMap.put("1", fixed);
    fixedMap.put("2", fixed);
    record.put("mapOfFixedField", fixedMap);
    Map<String, Integer> integerMap = new HashMap<>();
    integerMap.put("1", 123);
    integerMap.put("2", 124);
    Map<String, String> integerStringMap = new HashMap<>();
    integerStringMap.put("1", "123");
    integerStringMap.put("2", "124");
    record.put("mapOfIntegerField", integerStringMap);
    Map<String, String> mapField = new HashMap<>();
    mapField.put("1", "abc" + suffix);
    mapField.put("2", "abc" + suffix);
    record.put("mapField", mapField);
    List<String> stringArrayField = new ArrayList<>();
    stringArrayField.add("first_entry_" + suffix);
    stringArrayField.add("second_entry_" + suffix);
    record.put("arrayField", stringArrayField);
    List<Float> floatArrayField = new ArrayList<>();
    floatArrayField.add(1.01f);
    floatArrayField.add(2.01f);
    record.put("floatArrayField", floatArrayField);

    GenericRecord nestedRecord = new GenericData.Record(SCHEMA_NESTED_RECORD);
    nestedRecord.put("stringField", "test_string" + suffix);
    nestedRecord.put("bytesField", ByteBuffer.wrap(("test_bytes" + suffix).getBytes()));
    nestedRecord.put("intField", 100);
    nestedRecord.put("BigIntegerField", "123");
    nestedRecord.put("fixedField", fixed);
    nestedRecord.put("mapOfFixedField", fixedMap);
    nestedRecord.put("mapOfIntegerField", integerStringMap);
    nestedRecord.put("mapField", mapField);
    nestedRecord.put("arrayField", stringArrayField);
    nestedRecord.put("floatArrayField", floatArrayField);
    record.put("nestedRecord", nestedRecord);

    return record;
  }

  private static Decoder getDecoder(byte[] bytes) {
    return DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
  }

  private static byte[] serialize(GenericRecord record, Schema schema) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);
    DatumWriter datumWriter = new GenericDatumWriter(schema);
    datumWriter.write(record, encoder);
    encoder.flush();

    return baos.toByteArray();
  }

  public void compareTwoRecords(GenericRecord record1, GenericRecord record2) throws IOException {
    if (Utils.isAvro14()) {
      // Avro-1.4 doesn't support map compare, so we just compare the serialized bytes instead
      Assert.assertEquals(serialize(record1, COMPLICATE_SCHEMA), serialize(record2, COMPLICATE_SCHEMA));
    } else {
      Assert.assertEquals(record1, record2);
    }
  }

  @Test(groups = {"deserializationTest"})
  public void testFastGenericDeserializerGenerator() throws Exception {
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    FastDeserializer<GenericRecord> deserializer =
        (FastDeserializer<GenericRecord>) cache.buildFastGenericDeserializer(COMPLICATE_SCHEMA, COMPLICATE_SCHEMA);

    // Generate a record
    GenericRecord record = newComplicateRecord('0');
    byte[] serializedBytes = serialize(record, COMPLICATE_SCHEMA);
    // Generate a different record
    GenericRecord reuseRecord = newComplicateRecord('1');


    GenericRecord deserializedRecordWithFastAvro = deserializer.deserialize(getDecoder(serializedBytes));
    GenericRecord deserializedRecordWithFastAvroWithReuse =
        deserializer.deserialize(deserializedRecordWithFastAvro, getDecoder(serializedBytes));
    compareTwoRecords(deserializedRecordWithFastAvro, deserializedRecordWithFastAvroWithReuse);

    DatumReader datumReader = new GenericDatumReader(COMPLICATE_SCHEMA);
    GenericRecord deserializedRecordWithRegularAvro =
        (GenericRecord) datumReader.read(null, getDecoder(serializedBytes));
    // regenerate the reuse record since the original one is modified in the last step.
    reuseRecord = newComplicateRecord('1');
    GenericRecord deserializedRecordWithRegularAvroWithReuse =
        (GenericRecord) datumReader.read(reuseRecord, getDecoder(serializedBytes));
    compareTwoRecords(deserializedRecordWithFastAvro, deserializedRecordWithRegularAvro);
    compareTwoRecords(deserializedRecordWithFastAvro, deserializedRecordWithRegularAvroWithReuse);
  }

  @Test(groups = {"deserializationTest"})
  public void testFastGenericDeserializerPrimitFloatList() throws Exception {
    String schemaString = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}, {\"name\":\"inventory\", \"type\" : {  \"type\" : \"array\", \"items\" : \"float\" }}] }";

    Schema oldRecordSchema = Schema.parse(schemaString);
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    FastDeserializer<GenericRecord> deserializer =
        (FastDeserializer<GenericRecord>) cache.buildFastGenericDeserializer(oldRecordSchema, oldRecordSchema);

    GenericData.Record record = new GenericData.Record(oldRecordSchema);
    ArrayList<Float> arrayList = new ArrayList();
    arrayList.add((float)10);
    arrayList.add((float)20);

    record.put("name", "test");
    record.put("inventory", arrayList);
    byte[] serializedBytes = serialize(record, oldRecordSchema);
    GenericRecord deserRecord = deserializer.deserialize(getDecoder(serializedBytes));

    // Generate a different record
    GenericData.Record record1 = new GenericData.Record(oldRecordSchema);
    record1.put("name", "test1");
    arrayList.add((float)30);
    record1.put("inventory", arrayList);
    serializedBytes = serialize(record1, oldRecordSchema);
    // generate a record to reuse bytebuffer
    GenericRecord genericRecord = deserializer.deserialize(deserRecord, getDecoder(serializedBytes));
    List<Float> list = (List<Float>)genericRecord.get(1);
    Assert.assertEquals(list.size(), 3);

    arrayList.clear();
    arrayList.add((float)10);
    record1.put("inventory", arrayList);
    serializedBytes = serialize(record1, oldRecordSchema);
    genericRecord = deserializer.deserialize(deserRecord, getDecoder(serializedBytes));
    list = (List<Float>)genericRecord.get(1);
    Assert.assertEquals(list.size(), 1);

  }
}
