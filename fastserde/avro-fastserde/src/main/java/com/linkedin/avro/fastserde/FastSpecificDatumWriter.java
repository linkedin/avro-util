package com.linkedin.avro.fastserde;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


/**
 * {@link org.apache.avro.specific.SpecificDatumWriter} backed by generated serialization code.
 */
public class FastSpecificDatumWriter<T> extends FastGenericDatumWriter<T, SpecificData> {

  public FastSpecificDatumWriter(Schema schema) {
    super(schema);
  }

  public FastSpecificDatumWriter(Schema schema, SpecificData modelData) {
    super(schema, modelData);
  }

  public FastSpecificDatumWriter(Schema schema, FastSerdeCache cache) {
    super(schema, cache);
  }

  public FastSpecificDatumWriter(Schema schema, SpecificData modelData, FastSerdeCache cache) {
    super(schema, modelData, cache);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FastSerializer<T> getFastSerializerFromCache(FastSerdeCache fastSerdeCache, Schema schema, SpecificData modelData) {
    return (FastSerializer<T>) fastSerdeCache.getFastSpecificSerializer(schema, modelData);
  }

  @Override
  protected FastSerializer<T> getRegularAvroImpl(Schema schema, SpecificData modelData) {
    return new FastSerdeCache.FastSerializerWithAvroSpecificImpl<>(schema, modelData);
  }
}
