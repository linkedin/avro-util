package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class FastDeserializerGeneratorTest {
  @Test
  public void testSchemaForFixedField() throws IOException, InterruptedException {
    if (Utils.isAvro14()) {
      throw new SkipException("Avro 1.4 doesn't have schemas for GenericFixed type");
    }

    Schema writerSchema = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"fixedField\",\"type\":{\"type\":\"fixed\",\"name\":\"FixedType\",\"size\":5}}]}");
    Schema readerSchema = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"fixedField\",\"type\":{\"type\":\"fixed\",\"name\":\"FixedType\",\"size\":5,\"newField\": \"New field to change something\"}}]}");

    GenericRecord writtenRecord = new GenericData.Record(writerSchema);
    writtenRecord.put("fixedField", AvroCompatibilityHelper.newFixed(writerSchema.getField("fixedField").schema(), new byte[]{1,2,3,4,5}));

    byte[] writeBytes = serialize(writtenRecord);

    FastGenericDatumReader datumReader = new FastGenericDatumReader(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
    while (!datumReader.isFastDeserializerUsed()) {
      deserialize(datumReader, writeBytes);
      Thread.sleep(100);
    }

    Object data = deserialize(datumReader, writeBytes);
    GenericFixed fixedField = ((GenericFixed) ((GenericData.Record) data).get("fixedField"));

    Schema fixedFieldSchema = AvroSchemaUtil.getDeclaredSchema(fixedField);
    Assert.assertNotNull(fixedFieldSchema, "Schema for field must always be set.");
  }

  private byte[] serialize(GenericRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(baos, false, null);

    DatumWriter datumWriter = new FastGenericDatumWriter(record.getSchema(), FastSerdeCache.getDefaultInstance());
    datumWriter.write(record, encoder);
    encoder.flush();

    return baos.toByteArray();
  }

  private Object deserialize(DatumReader datumReader, byte[] bytes) throws IOException {
    return datumReader.read(null, AvroCompatibilityHelper.newBinaryDecoder(bytes));
  }
}
