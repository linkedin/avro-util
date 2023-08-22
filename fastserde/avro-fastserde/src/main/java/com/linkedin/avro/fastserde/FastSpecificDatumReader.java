package com.linkedin.avro.fastserde;

import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


/**
 * {@link org.apache.avro.specific.SpecificDatumReader} backed by generated deserialization code.
 */
public class FastSpecificDatumReader<T> extends FastGenericDatumReader<T, SpecificData> {

  public FastSpecificDatumReader(Schema schema) {
    super(schema, schema);
  }

  public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
    super(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
  }

  public FastSpecificDatumReader(Schema schema, FastSerdeCache cache) {
    super(schema, schema, cache);
  }

  public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache) {
    super(writerSchema, readerSchema, cache);
  }

  public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache, SpecificData modelData) {
    super(writerSchema, readerSchema, cache, modelData);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FastDeserializer<T> getFastDeserializerFromCache(FastSerdeCache fastSerdeCache, Schema writeSchema,
      Schema readerSchema, SpecificData modelData) {
    return (FastDeserializer<T>) fastSerdeCache.getFastSpecificDeserializer(writeSchema, readerSchema, modelData);
  }

  @Override
  protected CompletableFuture<FastDeserializer<T>> getFastDeserializer(FastSerdeCache fastSerdeCache,
      Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    return fastSerdeCache.getFastSpecificDeserializerAsync(writerSchema, readerSchema, modelData)
        .thenApply(d -> (FastDeserializer<T>) d);
  }

  @Override
  protected FastDeserializer<T> getRegularAvroImpl(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    return new FastSerdeCache.FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema, modelData);
  }
}
