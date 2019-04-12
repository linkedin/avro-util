package com.linkedin.avro.fastserde.micro.benchmark;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;


public class AvroGenericDeserializer<V> {

  /**
   * This default is intended for the backend. The client overrides this, and its default is defined in
   * the ClientConfig class, along with all the other client defaults.
   */
  private final DatumReader<V> datumReader;

  public AvroGenericDeserializer(Schema writer, Schema reader) {
    this(new GenericDatumReader<>(writer, reader));
  }

  public AvroGenericDeserializer(DatumReader<V> datumReader) {
    this.datumReader = datumReader;
  }

  public V deserialize(byte[] bytes) throws Exception {
    return deserialize(null, bytes);
  }

  public V deserialize(V reuseRecord, byte[] bytes) throws Exception {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    return deserialize(reuseRecord, decoder);
  }

  public V deserialize(BinaryDecoder decoder) throws Exception {
    return deserialize(null, decoder);
  }

  public V deserialize(V reuseRecord, BinaryDecoder decoder) throws Exception {
    try {
      return datumReader.read(reuseRecord, decoder);
    } catch (Exception e) {
      throw new RuntimeException("Could not deserialize bytes back into Avro object", e);
    }
  }

  public Iterable<V> deserializeObjects(byte[] bytes) throws Exception {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    return deserializeObjects(DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null));
  }

  public Iterable<V> deserializeObjects(BinaryDecoder decoder) throws Exception {

    List<V> objects = new ArrayList();
    try {
      while (!decoder.isEnd()) {
        objects.add(datumReader.read(null, decoder));
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not deserialize bytes back into Avro objects", e);
    }

    return objects;
  }
}
