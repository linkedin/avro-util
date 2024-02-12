package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic {@link DatumWriter} backed by generated serialization code.
 */
public class FastGenericDatumWriter<T> implements DatumWriter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastGenericDatumWriter.class);
  private Schema writerSchema;
  private final GenericData modelData;
  protected final FastSerdeCache cache;
  private final DatumWriterCustomization customization;
  private final FastSerializer<T> coldSerializer;
  private FastSerializer<T> cachedFastSerializer;

  public FastGenericDatumWriter(Schema schema) {
    this(schema, null, null);
  }

  public FastGenericDatumWriter(Schema schema, GenericData modelData) {
    this(schema, modelData, null);
  }

  public FastGenericDatumWriter(Schema schema, FastSerdeCache cache) {
    this(schema, null, cache);
  }

  public FastGenericDatumWriter(Schema schema, GenericData modelData, FastSerdeCache cache) {
    this(schema, modelData, cache, null);
  }
  public FastGenericDatumWriter(Schema schema, GenericData modelData, FastSerdeCache cache, DatumWriterCustomization customization) {
    this.writerSchema = schema;
    this.modelData = modelData;
    this.customization = customization == null ? DatumWriterCustomization.DEFAULT_DATUM_WRITER_CUSTOMIZATION : customization;
    this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();
    this.coldSerializer = getRegularAvroImpl(writerSchema, modelData, this.customization);
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      this.cachedFastSerializer = coldSerializer;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Current avro version: " + Utils.getRuntimeAvroVersion() + " is not supported, and only the following"
                + " versions are supported: " + Utils.getAvroVersionsSupportedForSerializer()
                + ", so will skip the FastSerializer generation");
      }
    } else if (!FastSerdeCache.isSupportedForFastSerializer(schema.getType())) {
      // For unsupported schema type, we won't try to fetch it from FastSerdeCache since it is inefficient.
      this.cachedFastSerializer = coldSerializer;
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
  public void write(T data, Encoder out) throws IOException {
    FastSerializer<T> fastSerializer;
    if (cachedFastSerializer != null) {
      fastSerializer = cachedFastSerializer;
    } else {
      fastSerializer = getFastSerializerFromCache(cache, writerSchema, modelData, customization);

      if (fastSerializer.hasDynamicClassGenerationDone()) {
        if (fastSerializer.isBackedByGeneratedClass()) {
          /*
           * Runtime class generation is done successfully, so cache it.
           */
          cachedFastSerializer = fastSerializer;
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("FastSerializer has been generated and cached for writer schema: [" + writerSchema + "]");
          }
        } else {
          /*
           * Runtime class generation fails, so this class will cache a newly generated cold deserializer, which will
           * honer {@link FastSerdeCache#isFailFast()}.
           */
          cachedFastSerializer = getRegularAvroImplWhenGenerationFail(writerSchema, modelData, customization);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("FastGenericDeserializer generation fails, and will cache cold serializer for writer schema: [" + writerSchema + "]");
          }
        }
        fastSerializer = cachedFastSerializer;
      } else {
        /*
         * Don't use the cached serializer since it may not support the passed customization.
         */
        fastSerializer = coldSerializer;
      }
    }

    fastSerializer.serialize(data, out, customization);
  }

  @SuppressWarnings("unchecked")
  protected FastSerializer<T> getFastSerializerFromCache(FastSerdeCache fastSerdeCache, Schema schema,
      GenericData modelData, DatumWriterCustomization customization) {
    return (FastSerializer<T>) fastSerdeCache.getFastGenericSerializer(schema, modelData, customization);
  }

  protected FastSerializer<T> getRegularAvroImpl(Schema schema, GenericData modelData,
      DatumWriterCustomization customization) {
    return new FastSerdeUtils.FastSerializerWithAvroGenericImpl<>(schema, modelData, customization, false);
  }

  protected FastSerializer<T> getRegularAvroImplWhenGenerationFail(Schema schema, GenericData modelData,
      DatumWriterCustomization customization) {
    return new FastSerdeUtils.FastSerializerWithAvroGenericImpl<>(schema, modelData, customization, cache.isFailFast(), true);
  }

  private static boolean isFastSerializer(FastSerializer<?> serializer) {
    return serializer.isBackedByGeneratedClass();
  }

  /**
   * @return flag to indicate whether fast serializer is being used or not
   */
  public boolean isFastSerializerUsed() {
    if (cachedFastSerializer == null) {
      return false;
    }
    return isFastSerializer(cachedFastSerializer);
  }
}
