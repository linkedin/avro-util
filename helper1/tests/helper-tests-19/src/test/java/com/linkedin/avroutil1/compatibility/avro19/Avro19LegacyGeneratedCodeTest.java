package com.linkedin.avroutil1.compatibility.avro19;

import by14.ExampleEnum;
import by14.ExampleFixed;
import by14.ExampleRecord;
import by14.SimpleEnum;
import by14.SimpleRecord;
import java.io.ByteArrayOutputStream;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.annotations.Test;


public class Avro19LegacyGeneratedCodeTest {

  @Test(expectedExceptions = AbstractMethodError.class)
  public void demonstrateAvro14FixedUnusableUnder19() throws Exception {
    //avro fixed classes extend org.apache.avro.specific.SpecificFixed which, in turn implements
    //org.apache.avro.generic.GenericFixed. in avro 1.5+ GenericFixed extends org.apache.avro.generic.GenericContainer.
    //GenericContainer, in turn, defined method getSchema() that avro-14-generated fixed classes dont implement
    new by14.SimpleFixed();
  }

//  @Test
//  public void demonstrateAvro14Record() throws Exception {
//    //TODO - delete this test
//    by14.SimpleRecord record = new SimpleRecord();
//    record.stringField = "bob";
//
//    runSerializationCycle(record);
//    int h = 8;
//  }

//  @Test
//  public void demonstrateAvro19CantHandleAvro14GeneratedCode() throws Exception {
//
//    by14.ExampleRecord record = new ExampleRecord();
//    record.enumField = ExampleEnum.A;
//    record.fixedField = new ExampleFixed();
//    record.fixedField.bytes(new byte[] {1, 2, 3, 4, 5, 6, 7});
//
//    runSerializationCycle(record);
//  }
//
//  private void runSerializationCycle(Object thingie) throws Exception {
//    ByteArrayOutputStream os = new ByteArrayOutputStream();
//    EncoderFactory encoderFactory = EncoderFactory.get();
//    BinaryEncoder binaryEncoder = encoderFactory.directBinaryEncoder(os, null);
//    @SuppressWarnings({"unchecked", "RedundantCast"})
//    SpecificDatumWriter<Object> writer = (SpecificDatumWriter<Object>) new SpecificDatumWriter<>(thingie.getClass());
//    writer.write(thingie, binaryEncoder);
//    binaryEncoder.flush();
//    byte[] serialized = os.toByteArray();
//
//    DecoderFactory decoderFactory = DecoderFactory.get();
//    BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(serialized, null);
//    @SuppressWarnings({"unchecked", "RedundantCast"})
//    SpecificDatumReader<Object> reader = (SpecificDatumReader<Object>) new SpecificDatumReader<>(thingie.getClass());
//    Object deserialized = reader.read(null, binaryDecoder);
//
//    int h = 7;
//  }

}
