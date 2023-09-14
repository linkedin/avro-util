package org.apache.avro.generic;

import org.apache.avro.Schema;

import com.linkedin.avro.fastserde.Utils;


/**
 * A light-weight extension of {@link GenericDatumReader} which merely ensures that the types of the
 * extended API are always returned.
 *
 * This class needs to be in the org.apache.avro.generic package in order to access protected methods.
 */
public class ColdGenericDatumReader<T> extends GenericDatumReader<T> implements ColdDatumReaderMixIn {

  public ColdGenericDatumReader(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    super(writerSchema, readerSchema, modelData != null ? modelData : GenericData.get());
  }

  public ColdGenericDatumReader(Schema writerSchema, Schema readerSchema) {
    super(writerSchema, readerSchema);
  }

  public static <T> ColdGenericDatumReader<T> of(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    if (Utils.isLogicalTypeSupported()) {
      return new ColdGenericDatumReader<>(writerSchema, readerSchema, modelData);
    } else {
      return new ColdGenericDatumReader<>(writerSchema, readerSchema);
    }
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    return newArray(old, size, schema, super::newArray);
  }
}
