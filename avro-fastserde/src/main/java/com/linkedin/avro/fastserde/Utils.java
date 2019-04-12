package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import java.util.ArrayList;
import java.util.List;


public class Utils {
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER = new ArrayList<>();
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER = new ArrayList<>();

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
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER;
  }

  static {
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_4);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_8);

    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_8);
  }
}
