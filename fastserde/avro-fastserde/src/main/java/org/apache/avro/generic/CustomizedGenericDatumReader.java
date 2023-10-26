package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;

/**
 * This class will apply {@link DatumReaderCustomization} at runtime.
 */
public class CustomizedGenericDatumReader<T> extends GenericDatumReader<T> implements ColdDatumReaderMixIn {
  private final DatumReaderCustomization customization;

  public CustomizedGenericDatumReader(Schema writerSchema, Schema readerSchema, GenericData modelData, DatumReaderCustomization customization) {
    super(writerSchema, readerSchema, modelData != null ? modelData : GenericData.get());
    if (customization == null) {
      throw new IllegalArgumentException("'customization' param should not null when constructing " +  this.getClass().getName());
    }
    this.customization = customization;
  }

  public CustomizedGenericDatumReader(Schema writerSchema, Schema readerSchema, DatumReaderCustomization customization) {
    super(writerSchema, readerSchema);
    if (customization == null) {
      throw new IllegalArgumentException("'customization' param should not null when constructing " +  this.getClass().getName());
    }
    this.customization = customization;
  }


  @Override
  protected Object newMap(Object old, int size) {
    return customization.getNewMapOverrideFunc().apply(old, size);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    return newArray(old, size, schema, super::newArray);
  }
}
