package com.linkedin.avro.fastserde;

import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * {@link org.apache.avro.specific.SpecificDatumReader} backed by generated deserialization code.
 */
public class FastSpecificDatumReader<T> extends FastGenericDatumReader<T> {
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

  @Override
  protected FastDeserializer<T> getFastDeserializerFromCache(FastSerdeCache fastSerdeCache, Schema writeSchema,
      Schema readerSchema) {
    return (FastDeserializer<T>) fastSerdeCache.getFastSpecificDeserializer(writeSchema, readerSchema);
  }

  @Override
  protected CompletableFuture<FastDeserializer<T>> getFastDeserializer(FastSerdeCache fastSerdeCache,
      Schema writerSchema, Schema readerSchema) {
    return fastSerdeCache.getFastSpecificDeserializerAsync(writerSchema, readerSchema)
        .thenApply(d -> (FastDeserializer<T>) d);
  }

  @Override
  protected FastDeserializer<T> getRegularAvroImpl(Schema writerSchema, Schema readerSchema) {
    return new FastSerdeCache.FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema);
  }
}
