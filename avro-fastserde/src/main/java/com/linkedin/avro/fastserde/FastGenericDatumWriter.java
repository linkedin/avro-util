package com.linkedin.avro.fastserde;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;


/**
 * Generic {@link DatumWriter} backed by generated serialization code.
 */
public class FastGenericDatumWriter<T> implements DatumWriter<T> {
  private static final Logger LOGGER = Logger.getLogger(FastGenericDatumWriter.class);
  private final FastSerdeCache cache;
  private Schema writerSchema;
  private FastSerializer<T> cachedFastSerializer;

  public FastGenericDatumWriter(Schema schema) {
    this(schema, FastSerdeCache.getDefaultInstance());
  }

  public FastGenericDatumWriter(Schema schema, FastSerdeCache cache) {
    this.writerSchema = schema;
    this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      this.cachedFastSerializer = getRegularAvroImpl(writerSchema);
      LOGGER.info("Current avro version: " + Utils.getRuntimeAvroVersion() + " is not supported, and only the following"
          + " versions are supported: " + Utils.getAvroVersionsSupportedForSerializer()
          + ", so will skip the FastSerializer generation");
    } else if (!FastSerdeCache.isSupportedForFastSerializer(schema.getType())) {
      // For unsupported schema type, we won't try to fetch it from FastSerdeCache since it is inefficient.
      this.cachedFastSerializer = getRegularAvroImpl(writerSchema);
      LOGGER.info("Skip the FastGenericSerializer generation since read schema type: " + schema.getType()
          + " is not supported");
    }
  }

  @Override
  public void setSchema(Schema schema) {
    writerSchema = schema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(T data, Encoder out) throws IOException {
    FastSerializer<T> fastSerializer;
    if (cachedFastSerializer != null) {
      fastSerializer = cachedFastSerializer;
    } else {
      fastSerializer = getFastSerializerFromCache(cache, writerSchema);
      if (fastSerializer instanceof FastSerdeCache.FastSerializerWithAvroSpecificImpl
          || fastSerializer instanceof FastSerdeCache.FastSerializerWithAvroGenericImpl) {
        // don't cache
      } else {
        cachedFastSerializer = fastSerializer;
        LOGGER.info("FastSerializer has been generated and cached for writer schema: [" + writerSchema + "]");
      }
    }

    fastSerializer.serialize(data, out);
  }

  protected FastSerializer<T> getFastSerializerFromCache(FastSerdeCache fastSerdeCache, Schema schema) {
    return (FastSerializer<T>) fastSerdeCache.getFastGenericSerializer(schema);
  }

  protected FastSerializer<T> getRegularAvroImpl(Schema schema) {
    return new FastSerdeCache.FastSerializerWithAvroGenericImpl<>(schema);
  }
}
