package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import com.linkedin.avro.compatibility.SchemaNormalization;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;


public class Utils {
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER = new ArrayList<>();
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER = new ArrayList<>();

  // Cache the mapping between Schema and the corresponding fingerprint
  private static final Map<Schema, Long> SCHEMA_IDS_CACHE = new ConcurrentHashMap<>();

  private Utils() {
  }

  public static boolean isAvro14() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_4);
  }

  public static boolean isSupportedAvroVersionsForDeserializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());
  }

  public static boolean isSupportedAvroVersionsForSerializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());

  }

  public static AvroVersion getRuntimeAvroVersion() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion();
  }

  public static List<AvroVersion> getAvroVersionsSupportedForDeserializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER;
  }

  public static List<AvroVersion> getAvroVersionsSupportedForSerializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER;
  }

  public static String generateSourcePathFromPackageName(String packageName) {
    StringBuilder pathBuilder = new StringBuilder(File.separator);
    Arrays.stream(packageName.split("\\.")).forEach( s -> pathBuilder.append(s).append(File.separator));
    return pathBuilder.toString();
  }

  /**
   * This function will produce a fingerprint for the provided schema.
   * @param schema
   * @return
   */
  public static Long getSchemaFingerprint(Schema schema) {
    Long schemaId = SCHEMA_IDS_CACHE.get(schema);
    if (schemaId == null) {
      schemaId = SchemaNormalization.parsingFingerprint64(schema);
      SCHEMA_IDS_CACHE.put(schema, schemaId);
    }

    return schemaId;
  }

  static {
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_4);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_8);

    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_8);
  }
}
