package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastSerdeCacheTest {

  @Test(groups = "deserializationTest")
  public void testIsSupportedForFastDeserializer() {
    Set<Schema.Type> supportedSchemaTypes = new HashSet<>();
    supportedSchemaTypes.add(Schema.Type.RECORD);
    supportedSchemaTypes.add(Schema.Type.MAP);
    supportedSchemaTypes.add(Schema.Type.ARRAY);

    Map<Schema.Type, Schema> schemaTypes = new HashMap<>();
    /**
     * Those types could be created by {@link Schema#create(org.apache.avro.Schema.Type)} function.
     */
    schemaTypes.put(Schema.Type.RECORD, Schema.parse("{\"type\": \"record\", \"name\": \"test\", \"fields\":[]}"));
    schemaTypes.put(Schema.Type.MAP, Schema.parse("{\"type\": \"map\", \"values\": \"string\"}"));
    schemaTypes.put(Schema.Type.ARRAY, Schema.parse("{\"type\": \"array\", \"items\": \"string\"}"));
    schemaTypes.put(Schema.Type.ENUM, Schema.parse("{\"type\": \"enum\", \"name\": \"test_enum\", \"symbols\":[]}"));
    schemaTypes.put(Schema.Type.UNION, Schema.parse("[\"null\", \"string\"]"));
    schemaTypes.put(Schema.Type.FIXED, Schema.parse("{\"type\": \"fixed\", \"size\": 16, \"name\": \"test_fixed\"}"));

    for (Schema.Type type : Schema.Type.values()) {
      Schema schema = schemaTypes.containsKey(type) ? schemaTypes.get(type) : Schema.create(type);
      if (supportedSchemaTypes.contains(type)) {
        Assert.assertTrue(FastSerdeCache.isSupportedForFastDeserializer(type));
        FastDeserializerGeneratorBase.getClassName(schema, schema, "");
      } else {
        Assert.assertFalse(FastSerdeCache.isSupportedForFastDeserializer(type));
      }
    }
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastGenericDeserializerSurviveFromWrongClasspath() {
    String wrongClasspath = ".";
    FastSerdeCache cache = new FastSerdeCache(wrongClasspath);
    Schema testRecord = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    cache.buildFastGenericDeserializer(testRecord, testRecord, null);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastGenericDeserializerWithCorrectClasspath() {
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    Schema testRecord = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    cache.buildFastGenericDeserializer(testRecord, testRecord, null);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastSpecificDeserializerSurviveFromWrongClasspath() {
    String wrongClasspath = ".";
    FastSerdeCache cache = new FastSerdeCache(wrongClasspath);
    cache.buildFastSpecificDeserializer(TestRecord.SCHEMA$, TestRecord.SCHEMA$, null);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastSpecificDeserializerWithCorrectClasspath() {
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    cache.buildFastSpecificDeserializer(TestRecord.SCHEMA$, TestRecord.SCHEMA$, null);
  }
}
