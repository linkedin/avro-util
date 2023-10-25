package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;


/**
 * In Avro-1.4, {@link org.apache.avro.generic.GenericDatumReader} doesn't have a constructor
 * to take {@link org.apache.avro.generic.GenericData}.
 */
public class CustomizedGenericDatumReaderForAvro14<T> extends CustomizedGenericDatumReader<T> {

  public CustomizedGenericDatumReaderForAvro14(Schema writerSchema, Schema readerSchema,
      DatumReaderCustomization customization) {
    super(writerSchema, readerSchema, customization);
  }
}
