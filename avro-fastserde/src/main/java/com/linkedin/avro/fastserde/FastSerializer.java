package com.linkedin.avro.fastserde;

import java.io.IOException;
import org.apache.avro.io.Encoder;


public interface FastSerializer<T> {

  void serialize(T data, Encoder e) throws IOException;
}
