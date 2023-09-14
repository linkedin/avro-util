package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import com.linkedin.avro.fastserde.Utils;


/**
 * A light-weight extension of {@link SpecificDatumReader} which merely ensures that the types of
 * the extended API are always returned.
 *
 * This class needs to be in the org.apache.avro.generic package in order to access protected methods.
 */
public class ColdSpecificDatumReader<T> extends SpecificDatumReader<T> implements ColdDatumReaderMixIn {

  public ColdSpecificDatumReader(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    super(writerSchema, readerSchema, modelData != null ? modelData : SpecificData.get());
  }

  public ColdSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
    super(writerSchema, readerSchema);
  }

  public static <T> ColdSpecificDatumReader<T> of(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    if (Utils.isLogicalTypeSupported()) {
      return new ColdSpecificDatumReader<>(writerSchema, readerSchema, modelData);
    } else {
      return new ColdSpecificDatumReader<>(writerSchema, readerSchema);
    }
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    return newArray(old, size, schema, super::newArray);
  }
}
