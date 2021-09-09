package com.linkedin.avro.fastserde;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic {@link DatumWriter} backed by generated serialization code.
 */
public class FastGenericDatumWriter<T> implements DatumWriter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastGenericDatumWriter.class);
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Current avro version: " + Utils.getRuntimeAvroVersion() + " is not supported, and only the following"
                + " versions are supported: " + Utils.getAvroVersionsSupportedForSerializer()
                + ", so will skip the FastSerializer generation");
      }
    } else if (!FastSerdeCache.isSupportedForFastSerializer(schema.getType())) {
      // For unsupported schema type, we won't try to fetch it from FastSerdeCache since it is inefficient.
      this.cachedFastSerializer = getRegularAvroImpl(writerSchema);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skip the FastGenericSerializer generation since read schema type: " + schema.getType()
            + " is not supported");
      }
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
      if (!isFastSerializer(fastSerializer)) {
        // don't cache
      } else {
        cachedFastSerializer = fastSerializer;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("FastSerializer has been generated and cached for writer schema: [" + writerSchema + "]");
        }
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

  private static boolean isFastSerializer(FastSerializer serializer) {
    return !(serializer instanceof FastSerdeCache.FastSerializerWithAvroSpecificImpl
        || serializer instanceof FastSerdeCache.FastSerializerWithAvroGenericImpl);
  }

  /**
   * Return a flag to indicate whether fast serializer is being used or not.
   * @return
   */
  public boolean isFastSerializerUsed() {
    if (cachedFastSerializer == null) {
      return false;
    }
    return isFastSerializer(cachedFastSerializer);
  }
}
