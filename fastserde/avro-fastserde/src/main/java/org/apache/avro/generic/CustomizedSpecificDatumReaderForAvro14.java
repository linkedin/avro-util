package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;

/**
 * In Avro-1.4, {@link org.apache.avro.specific.SpecificDatumReader} doesn't have a constructor
 * to take {@link org.apache.avro.specific.SpecificData}.
 */
public class CustomizedSpecificDatumReaderForAvro14<T> extends CustomizedSpecificDatumReader<T> {
  public CustomizedSpecificDatumReaderForAvro14(Schema writerSchema, Schema readerSchema,
      DatumReaderCustomization customization) {
    super(writerSchema, readerSchema, customization);
  }
}
