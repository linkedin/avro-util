package com.linkedin.avro.fastserde;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic {@link DatumReader} backed by generated deserialization code.
 */
public class FastGenericDatumReader<T> implements DatumReader<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastGenericDatumReader.class);

  private Schema writerSchema;
  private Schema readerSchema;
  private final FastSerdeCache cache;
  private final GenericData modelData;

  private final AtomicReference<FastDeserializer<T>> cachedFastDeserializer = new AtomicReference<>();

  public FastGenericDatumReader(Schema schema) {
    this(schema, schema);
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema) {
    this(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    this(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance(), modelData);
  }

  public FastGenericDatumReader(Schema schema, FastSerdeCache cache) {
    this(schema, schema, cache);
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache) {
    this(writerSchema, readerSchema, cache, null);
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache, GenericData modelData) {
    this.writerSchema = writerSchema;
    this.readerSchema = readerSchema;
    this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();
    this.modelData = modelData;

    if (!Utils.isSupportedAvroVersionsForDeserializer()) {
      this.cachedFastDeserializer.set(getRegularAvroImpl(writerSchema, readerSchema, modelData));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Current avro version: " + Utils.getRuntimeAvroVersion() + " is not supported, and only the following"
                + " versions are supported: " + Utils.getAvroVersionsSupportedForDeserializer()
                + ", so skip the FastDeserializer generation");
      }
    } else if (!FastSerdeCache.isSupportedForFastDeserializer(readerSchema.getType())) {
      // For unsupported schema type, we won't try to fetch it from FastSerdeCache since it is inefficient.
      this.cachedFastDeserializer.set(getRegularAvroImpl(writerSchema, readerSchema, modelData));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skip the FastGenericDeserializer generation since read schema type: " + readerSchema.getType()
            + " is not supported");
      }
    }
  }

  @Override
  public void setSchema(Schema schema) {
    if (writerSchema == null) {
      writerSchema = schema;
    }

    if (readerSchema == null) {
      readerSchema = writerSchema;
    }
  }

  @Override
  public T read(T reuse, Decoder in) throws IOException {
    FastDeserializer<T> fastDeserializer;

    if (cachedFastDeserializer.get() != null) {
      fastDeserializer = cachedFastDeserializer.get();
    } else {
      fastDeserializer = getFastDeserializerFromCache(cache, writerSchema, readerSchema, modelData);
      if (FastSerdeCache.isFastDeserializer(fastDeserializer)) {
        cachedFastDeserializer.compareAndSet(null, fastDeserializer);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("FastGenericDeserializer was generated and cached for reader schema: ["
              + readerSchema + "], writer schema: [" + writerSchema + "]");
        }
      }
    }

    return fastDeserializer.deserialize(reuse, in);
  }

  public CompletableFuture<FastDeserializer<T>> getFastDeserializer() {
    return cachedFastDeserializer.get() != null ? CompletableFuture.completedFuture(cachedFastDeserializer.get())
        : getFastDeserializer(cache, writerSchema, readerSchema, modelData).thenApply(d -> {
          cachedFastDeserializer.compareAndSet(null, d);
          return d;
        });
  }

  protected CompletableFuture<FastDeserializer<T>> getFastDeserializer(FastSerdeCache fastSerdeCache,
      Schema writerSchema, Schema readerSchema, GenericData modelData) {
    return fastSerdeCache.getFastGenericDeserializerAsync(writerSchema, readerSchema, modelData)
        .thenApply(d -> (FastDeserializer<T>) d);
  }

  @SuppressWarnings("unchecked")
  protected FastDeserializer<T> getFastDeserializerFromCache(FastSerdeCache fastSerdeCache, Schema writerSchema,
      Schema readerSchema, GenericData modelData) {
    return (FastDeserializer<T>) fastSerdeCache.getFastGenericDeserializer(writerSchema, readerSchema, modelData);
  }

  protected FastDeserializer<T> getRegularAvroImpl(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    return new FastSerdeCache.FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema, modelData);
  }

  /**
   * Return a flag to indicate whether fast deserializer is being used or not.
   * @return true if fast deserializer is being used.
   */
  public boolean isFastDeserializerUsed() {
    FastDeserializer<T> fastDeserializer = cachedFastDeserializer.get();
    return fastDeserializer != null && FastSerdeCache.isFastDeserializer(fastDeserializer);
  }
}
