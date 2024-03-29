package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;


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

  public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    super(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance(), modelData);
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
      Schema readerSchema, GenericData specificData, DatumReaderCustomization customization) {
    return (FastDeserializer<T>) fastSerdeCache.getFastSpecificDeserializer(writeSchema, readerSchema, (SpecificData) specificData, customization);
  }

  @Override
  protected CompletableFuture<FastDeserializer<T>> getFastDeserializer(FastSerdeCache fastSerdeCache,
      Schema writerSchema, Schema readerSchema, GenericData specificData, DatumReaderCustomization customization) {
    return fastSerdeCache.getFastSpecificDeserializerAsync(writerSchema, readerSchema, (SpecificData) specificData, customization)
        .thenApply(d -> (FastDeserializer<T>) d);
  }

  @Override
  protected FastDeserializer<T> getRegularAvroImpl(Schema writerSchema, Schema readerSchema,
      GenericData modelData, DatumReaderCustomization customization) {
    return new FastSerdeUtils.FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema,
        (SpecificData)modelData, customization, false);
  }

  protected FastDeserializer<T> getRegularAvroImplWhenGenerationFail(Schema writerSchema, Schema readerSchema,
      GenericData modelData, DatumReaderCustomization customization) {
    return new FastSerdeUtils.FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema,
        (SpecificData)modelData, customization, cache.isFailFast(), true);
  }
}
