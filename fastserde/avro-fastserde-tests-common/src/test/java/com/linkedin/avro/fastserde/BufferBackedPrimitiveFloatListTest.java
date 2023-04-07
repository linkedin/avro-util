package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.primitive.PrimitiveFloatArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BufferBackedPrimitiveFloatListTest {
  @Test
  public void testPrimitiveFloatArrayCache() throws IOException {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.FLOAT));
    List<Float> testFloatArray = Arrays.asList(1.0f, 2.0f, 3.0f);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);
    try {
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(arraySchema);
      writer.write(testFloatArray, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
    BufferBackedPrimitiveFloatList list = (BufferBackedPrimitiveFloatList) BufferBackedPrimitiveFloatList
        .readPrimitiveFloatArray(new Object(), decoder);
    //Remove method will populate the cache using VH, read value by index
    //invalidate cache and return value, we need to make sure we read values correctly
    Assert.assertEquals(list.remove(2), 3.0f);
    }
}
