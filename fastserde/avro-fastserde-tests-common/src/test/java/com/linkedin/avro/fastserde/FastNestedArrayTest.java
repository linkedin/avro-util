package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.NestedArrayItem;
import com.linkedin.avro.fastserde.generated.avro.NestedArrayTest;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastNestedArrayTest {

  @Test
  public void testExample() throws Exception {

     NestedArrayTest nestedArrayTest = new NestedArrayTest();

    List<NestedArrayItem> items = new ArrayList<>();
    NestedArrayItem item = new NestedArrayItem();
    item.setItemName("itemName");
    items.add(item);

    nestedArrayTest.setNestedArrayItems(Collections.singletonList(items));

    SpecificDatumWriter<NestedArrayTest> writer =
        new SpecificDatumWriter<>(NestedArrayTest.SCHEMA$);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder =
        AvroCompatibilityHelper.newJsonEncoder(NestedArrayTest.SCHEMA$, os, true);
    writer.write(nestedArrayTest, encoder);
    encoder.flush();

    FastSpecificDatumReader<NestedArrayTest> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(NestedArrayTest.SCHEMA$);
    FastDeserializer<NestedArrayTest> fastDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get();

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    Decoder decoder =
        DecoderFactory.defaultFactory().jsonDecoder(NestedArrayTest.SCHEMA$, is);
    NestedArrayTest actual = fastDeserializer.deserialize(decoder);

    Assert.assertEquals(actual, nestedArrayTest);
  }
}
