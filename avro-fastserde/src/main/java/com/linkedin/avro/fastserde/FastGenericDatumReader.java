package com.linkedin.avro.fastserde;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
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
  private FastSerdeCache cache;

  private final AtomicReference<FastDeserializer<T>> cachedFastDeserializer = new AtomicReference<>();

  public FastGenericDatumReader(Schema schema) {
    this(schema, schema);
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema) {
    this(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
  }

  public FastGenericDatumReader(Schema schema, FastSerdeCache cache) {
    this(schema, schema, cache);
  }

  public FastGenericDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache) {
    this.writerSchema = writerSchema;
    this.readerSchema = readerSchema;
    this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();

    if (!Utils.isSupportedAvroVersionsForDeserializer()) {
      this.cachedFastDeserializer.set(getRegularAvroImpl(writerSchema, readerSchema));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Current avro version: " + Utils.getRuntimeAvroVersion() + " is not supported, and only the following"
                + " versions are supported: " + Utils.getAvroVersionsSupportedForDeserializer()
                + ", so skip the FastDeserializer generation");
      }
    } else if (!FastSerdeCache.isSupportedForFastDeserializer(readerSchema.getType())) {
      // For unsupported schema type, we won't try to fetch it from FastSerdeCache since it is inefficient.
      this.cachedFastDeserializer.set(getRegularAvroImpl(writerSchema, readerSchema));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skip the FastGenericDeserializer generation since read schema type: " + readerSchema.getType()
            + " is not supported");
      }
    }
  }

  public void warmUp(Duration period, Duration timeout) {
    if (cachedFastDeserializer.get() != null) {
      return;
    }

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    ScheduledFuture<?> updateTask = executor.scheduleWithFixedDelay(() -> {
      FastDeserializer<T> fastDeserializer = getFastDeserializerFromCache(cache, writerSchema, readerSchema);
      if (cachedFastDeserializer.get() == null && isFastDeserializer(fastDeserializer)) {
        cachedFastDeserializer.compareAndSet(null, fastDeserializer);
      }
    }, 0, period.toMillis(), TimeUnit.MILLISECONDS);

    try {
      executor.scheduleWithFixedDelay(() -> {
        if (cachedFastDeserializer.get() != null) {
          updateTask.cancel(true);
          // early termination
          executor.shutdown();
        }
      }, 10, period.toMillis(), TimeUnit.MILLISECONDS).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (cachedFastDeserializer.get() == null) {
        LOGGER.warn("Failed to warm up Fast Deserializer", e);
      }
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
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
  @SuppressWarnings("unchecked")
  public T read(T reuse, Decoder in) throws IOException {
    FastDeserializer<T> fastDeserializer = null;

    if (cachedFastDeserializer.get() != null) {
      fastDeserializer = cachedFastDeserializer.get();
    } else {
      fastDeserializer = getFastDeserializerFromCache(cache, writerSchema, readerSchema);
      if (!isFastDeserializer(fastDeserializer)) {
        // don't cache
      } else {
        cachedFastDeserializer.compareAndSet(null, fastDeserializer);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("FastGenericDeserializer was generated and cached for reader schema: ["
              + readerSchema + "], writer schema: [" + writerSchema + "]");
        }
      }
    }

    return fastDeserializer.deserialize(reuse, in);
  }

  protected FastDeserializer<T> getFastDeserializerFromCache(FastSerdeCache fastSerdeCache, Schema writerSchema,
      Schema readerSchema) {
    return (FastDeserializer<T>) fastSerdeCache.getFastGenericDeserializer(writerSchema, readerSchema);
  }

  protected FastDeserializer<T> getRegularAvroImpl(Schema writerSchema, Schema readerSchema) {
    return new FastSerdeCache.FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema);
  }

  private static boolean isFastDeserializer(FastDeserializer deserializer) {
    return !(deserializer instanceof FastSerdeCache.FastDeserializerWithAvroSpecificImpl
        || deserializer instanceof FastSerdeCache.FastDeserializerWithAvroGenericImpl);
  }

  /**
   * Return a flag to indicate whether fast deserializer is being used or not.
   * @return
   */
  public boolean isFastDeserializerUsed() {
    if (cachedFastDeserializer.get() == null) {
      return false;
    }
    return isFastDeserializer(cachedFastDeserializer.get());
  }
}
