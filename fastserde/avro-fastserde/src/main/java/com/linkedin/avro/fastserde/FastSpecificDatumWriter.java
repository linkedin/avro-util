package com.linkedin.avro.fastserde;

import org.apache.avro.Schema;


/**
 * {@link org.apache.avro.specific.SpecificDatumWriter} backed by generated serialization code.
 */
public class FastSpecificDatumWriter<T> extends FastGenericDatumWriter<T> {
  public FastSpecificDatumWriter(Schema schema) {
    super(schema);
  }

  public FastSpecificDatumWriter(Schema schema, FastSerdeCache cache) {
    super(schema, cache);
  }

  @Override
  protected FastSerializer<T> getFastSerializerFromCache(FastSerdeCache fastSerdeCache, Schema schema) {
    return (FastSerializer<T>) fastSerdeCache.getFastSpecificSerializer(schema);
  }

  @Override
  protected FastSerializer<T> getRegularAvroImpl(Schema schema) {
    return new FastSerdeCache.FastSerializerWithAvroSpecificImpl<>(schema);
  }
}
