package com.linkedin.avro.fastserde;

import java.io.IOException;
import org.apache.avro.io.Decoder;


public interface FastDeserializer<Type> {

  default Type deserialize(Decoder d) throws IOException {
    return deserialize(null, d);
  }

  Type deserialize(Type reuse, Decoder d) throws IOException;
}
