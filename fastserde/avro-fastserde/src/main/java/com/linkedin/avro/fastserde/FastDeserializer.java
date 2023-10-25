package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.Decoder;

import static com.linkedin.avro.fastserde.customized.DatumReaderCustomization.*;


public interface FastDeserializer<T> extends FastClassStatus {

  default T deserialize(Decoder d) throws IOException {
    return deserialize(null, d);
  }

  default T deserialize(T reuse, Decoder d) throws IOException {
    return deserialize(reuse, d, DEFAULT_DATUM_READER_CUSTOMIZATION);
  }

  T deserialize(T reuse, Decoder d, DatumReaderCustomization customization) throws IOException;
}
