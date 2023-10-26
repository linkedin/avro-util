package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.io.Encoder;

import static com.linkedin.avro.fastserde.customized.DatumWriterCustomization.*;


public interface FastSerializer<T>  extends FastClassStatus {

  default void serialize(T data, Encoder e) throws IOException {
    serialize(data, e, DEFAULT_DATUM_WRITER_CUSTOMIZATION);
  }

  void serialize(T data, Encoder e, DatumWriterCustomization customization) throws IOException;
}
