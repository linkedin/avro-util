package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * A light-weight extension of {@link SpecificDatumReader} which merely ensures that the types of
 * the extended API are always returned.
 *
 * This class needs to be in the org.apache.avro.generic package in order to access protected methods.
 */
public class ColdSpecificDatumReader<T> extends SpecificDatumReader<T> implements ColdDatumReaderMixIn {
  public ColdSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
    super(writerSchema, readerSchema);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    return newArray(old, size, schema, super::newArray);
  }
}
