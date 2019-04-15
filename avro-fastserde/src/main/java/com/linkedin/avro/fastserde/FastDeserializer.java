package com.linkedin.avro.fastserde;

import java.io.IOException;
import org.apache.avro.io.Decoder;


public interface FastDeserializer<T> {

  default T deserialize(Decoder d) throws IOException {
    return deserialize(null, d);
  }

  T deserialize(T reuse, Decoder d) throws IOException;
}
