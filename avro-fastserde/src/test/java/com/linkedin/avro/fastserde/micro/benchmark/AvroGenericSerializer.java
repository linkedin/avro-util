package com.linkedin.avro.fastserde.micro.benchmark;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;


public class AvroGenericSerializer<K> {
  private final DatumWriter<K> datumWriter;

  public AvroGenericSerializer(Schema schema) {
    this(new GenericDatumWriter<>(schema));
  }

  protected AvroGenericSerializer(DatumWriter datumWriter) {
    this.datumWriter = datumWriter;
  }

  public byte[] serialize(K object) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(output);
    try {
      datumWriter.write(object, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Could not serialize the Avro object", e);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          System.err.println("Failed to close stream" + e);
        }
      }
    }
    return output.toByteArray();
  }

  public byte[] serializeObjects(Iterable<K> objects) throws Exception {
    return serializeObjects(objects, new ByteArrayOutputStream());
  }

  private byte[] serializeObjects(Iterable<K> objects, ByteArrayOutputStream output) throws Exception {
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(output);
    try {
      objects.forEach(object -> {
        try {
          datumWriter.write(object, encoder);
        } catch (IOException e) {
          throw new RuntimeException("Could not serialize the Avro object", e);
        }
      });
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Could not flush BinaryEncoder", e);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          System.err.println("Failed to close stream" + e);
        }
      }
    }
    return output.toByteArray();
  }

  public byte[] serializeObjects(Iterable<K> objects, ByteBuffer prefix) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(prefix.array(), prefix.position(), prefix.remaining());
    return serializeObjects(objects, output);
  }
}
