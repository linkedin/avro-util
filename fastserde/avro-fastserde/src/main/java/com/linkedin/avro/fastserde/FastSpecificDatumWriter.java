package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;


/**
 * {@link org.apache.avro.specific.SpecificDatumWriter} backed by generated serialization code.
 */
public class FastSpecificDatumWriter<T> extends FastGenericDatumWriter<T> {

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
  public FastSpecificDatumWriter(Schema schema, SpecificData modelData, FastSerdeCache cache,
      DatumWriterCustomization customization) {
    super(schema, modelData, cache, customization);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FastSerializer<T> getFastSerializerFromCache(FastSerdeCache fastSerdeCache, Schema schema,
      GenericData specificData, DatumWriterCustomization customization) {
    return (FastSerializer<T>) fastSerdeCache.getFastSpecificSerializer(schema, (SpecificData) specificData, customization);
  }

  @Override
  protected FastSerializer<T> getRegularAvroImpl(Schema schema, GenericData specificData,
      DatumWriterCustomization customization) {
    return new FastSerdeUtils.FastSerializerWithAvroSpecificImpl<>(schema, (SpecificData) specificData, customization, false);
  }

  protected FastSerializer<T> getRegularAvroImplWhenGenerationFail(Schema schema, GenericData modelData,
      DatumWriterCustomization customization) {
    return new FastSerdeUtils.FastSerializerWithAvroSpecificImpl<>(schema, (SpecificData) modelData, customization, cache.isFailFast(), true);
  }
}
