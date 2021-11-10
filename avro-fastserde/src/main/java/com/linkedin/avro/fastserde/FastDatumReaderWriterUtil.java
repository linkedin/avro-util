package com.linkedin.avro.fastserde;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.avro.Schema;

/**
 * Utility class to generate and reuse FastDatumReader/Writer. The cache key is schema object.
 *
 * Pre-requisite to use this util:
 * 1. Schema object will not be changed on-the-FLY, or the changes do NOT require new DatumReader/Writer;
 * 2. Your application will always use the same Schema object for the same schema;
 */
public class FastDatumReaderWriterUtil {

  protected static class SchemaPair {
    private final Schema writerSchema;
    private final Schema readerSchema;
    private final int hashCode;

    public SchemaPair(Schema writerSchema, Schema readerSchema) {
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
      this.hashCode = Objects.hash(System.identityHashCode(this.writerSchema), System.identityHashCode(this.readerSchema));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaPair that = (SchemaPair) o;
      return writerSchema == that.writerSchema && readerSchema == that.readerSchema;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

  //TODO :  LRU cache
  private static final Map<SchemaPair, FastGenericDatumReader<?>> fastGenericDatumReaderCache = new FastAvroConcurrentHashMap<>();
  private static final Map<Schema, FastGenericDatumWriter<?>> fastGenericDatumWriterCache = new WeakIdentityHashMap<>();

  private static final Map<SchemaPair, FastSpecificDatumReader<?>> fastSpecificDatumReaderCache = new FastAvroConcurrentHashMap<>();
  private static final Map<Schema, FastSpecificDatumWriter<?>> fastSpecificDatumWriterCache = new WeakIdentityHashMap<>();

  private static final Duration READER_WARM_UP_CHECK_PERIOD = Duration.ofMillis(100);
  private static final Duration READER_WARM_UP_TIME_OUT = Duration.ofSeconds(3);

  private FastDatumReaderWriterUtil() {
  }

  public static <T> FastGenericDatumReader<T> getFastGenericDatumReader(Schema schema) {
    return (FastGenericDatumReader<T>) getFastGenericDatumReader(schema, schema);
  }

  public static <T> FastGenericDatumReader<T> getFastGenericDatumReader(Schema writerSchema, Schema readerSchema) {
    SchemaPair schemaPair = new SchemaPair(writerSchema, readerSchema);
    return (FastGenericDatumReader<T>) fastGenericDatumReaderCache.computeIfAbsent(schemaPair, key -> new FastGenericDatumReader<>(writerSchema, readerSchema));
  }

  public static void warmUpFastGenericDatumReader(Schema writerSchema, Schema readerSchema, Duration period,
      Duration timeout) {
    getFastGenericDatumReader(writerSchema, readerSchema).warmUp(period, timeout);
  }

  public static void warmUpFastGenericDatumReader(Schema writerSchema, Schema readerSchema) {
    getFastGenericDatumReader(writerSchema, readerSchema).warmUp(READER_WARM_UP_CHECK_PERIOD, READER_WARM_UP_TIME_OUT);
  }

  public static <T> FastGenericDatumWriter<T> getFastGenericDatumWriter(Schema writerSchema) {
    FastGenericDatumWriter<T> fastDatumWriter = null;

    // lookup cache and read lock
    reentrantReadWriteLock.readLock().lock();
    try {
      fastDatumWriter = (FastGenericDatumWriter<T>)fastGenericDatumWriterCache.get(writerSchema);
    } finally {
      reentrantReadWriteLock.readLock().unlock();
    }
    // update cache and write lock
    if (fastDatumWriter == null) {
      reentrantReadWriteLock.writeLock().lock();
      try {
        fastDatumWriter = new FastGenericDatumWriter<>(writerSchema);
        fastGenericDatumWriterCache.put(writerSchema, fastDatumWriter);
      } finally {
        reentrantReadWriteLock.writeLock().unlock();
      }
    }
    return (FastGenericDatumWriter <T>) fastDatumWriter;
  }

  public static <T> FastSpecificDatumReader<T> getFastSpecificDatumReader(Schema schema) {
    return (FastSpecificDatumReader<T>) getFastSpecificDatumReader(schema, schema);
  }

  public static <T> FastSpecificDatumReader<T> getFastSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
    SchemaPair schemaPair = new SchemaPair(writerSchema, readerSchema);
    return (FastSpecificDatumReader<T>) fastSpecificDatumReaderCache.computeIfAbsent(schemaPair, key -> new FastSpecificDatumReader<>(writerSchema, readerSchema));
  }

  public static void warmUpFastSpecificDatumReader(Schema writerSchema, Schema readerSchema, Duration period,
      Duration timeout) {
    getFastSpecificDatumReader(writerSchema, readerSchema).warmUp(period, timeout);
  }

  public static void warmUpFastSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
    getFastSpecificDatumReader(writerSchema, readerSchema).warmUp(READER_WARM_UP_CHECK_PERIOD, READER_WARM_UP_TIME_OUT);
  }

  public static <T> FastSpecificDatumWriter<T> getFastSpecificDatumWriter(Schema writerSchema) {
    FastSpecificDatumWriter<T> fastDatumWriter = null;

    // lookup cache and read lock
    reentrantReadWriteLock.readLock().lock();
    try {
      fastDatumWriter = (FastSpecificDatumWriter<T>)fastSpecificDatumWriterCache.get(writerSchema);
    } finally {
      reentrantReadWriteLock.readLock().unlock();
    }
    // update cache and write lock
    if (fastDatumWriter == null) {
      reentrantReadWriteLock.writeLock().lock();
      try {
        fastDatumWriter = new FastSpecificDatumWriter<>(writerSchema);
        fastSpecificDatumWriterCache.put(writerSchema, fastDatumWriter);
      } finally {
        reentrantReadWriteLock.writeLock().unlock();
      }
    }
    return (FastSpecificDatumWriter<T>) fastDatumWriter;
  }
}
