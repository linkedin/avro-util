package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.NestedArrayItem;
import com.linkedin.avro.fastserde.generated.avro.NestedArrayTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.io.Decoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastNestedArrayTest {

  @Test
  public void testExample() throws Exception {

     NestedArrayTest nestedArrayTest = new NestedArrayTest();

    List<NestedArrayItem> items = new ArrayList<>();
    NestedArrayItem item = new NestedArrayItem();
    FastSerdeTestsSupport.setField(item, "itemName", "itemName");
    FastSerdeTestsSupport.setField(nestedArrayTest, "NestedArrayItems", Collections.singletonList(items));

    Decoder decoder = FastSerdeTestsSupport.specificDataAsDecoder(nestedArrayTest, NestedArrayTest.SCHEMA$);

    FastSpecificDatumReader<NestedArrayTest> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(NestedArrayTest.SCHEMA$);
    FastDeserializer<NestedArrayTest> fastDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get();

    NestedArrayTest actual = fastDeserializer.deserialize(decoder);

    Assert.assertEquals(actual, nestedArrayTest);
  }
}
