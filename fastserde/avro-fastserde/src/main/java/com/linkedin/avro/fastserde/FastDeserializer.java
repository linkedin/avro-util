package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import static com.linkedin.avro.fastserde.customized.DatumReaderCustomization.*;


public interface FastDeserializer<T> extends FastClassStatus {

  default T deserialize(Decoder d) throws IOException {
    return deserialize(null, d);
  }

  default T deserialize(T reuse, Decoder d) throws IOException {
    return deserialize(reuse, d, DEFAULT_DATUM_READER_CUSTOMIZATION);
  }

  /**
   * Set the writer's schema.
   * @see org.apache.avro.io.DatumReader#setSchema(Schema)
   */
  default void setSchema(Schema writerSchema) {
    // Implement this method only in vanilla-avro-based classes (e.g. fallback scenario).
    // Normally for generated deserializers it doesn't make sense.
    throw new UnsupportedOperationException("Can't change schema for already generated class.");
  }

  T deserialize(T reuse, Decoder d, DatumReaderCustomization customization) throws IOException;
}
