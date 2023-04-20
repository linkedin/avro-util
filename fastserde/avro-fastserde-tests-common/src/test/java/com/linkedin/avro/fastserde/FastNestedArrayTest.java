package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.NestedArrayItem;
import com.linkedin.avro.fastserde.generated.avro.NestedArrayTest;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.Decoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastNestedArrayTest {

  @Test
  public void testExample() throws Exception {

    NestedArrayTest nestedArrayTest = new NestedArrayTest();

    NestedArrayItem item = new NestedArrayItem();
    FastSerdeTestsSupport.setField(item, "ItemName", "itemName");

    List<NestedArrayItem> nestedArrayItem = new ArrayList<>();
    nestedArrayItem.add(item);
    List<List<NestedArrayItem>> nestedArrayItems = new ArrayList<>();
    nestedArrayItems.add(nestedArrayItem);

    FastSerdeTestsSupport.setField(nestedArrayTest, "NestedArrayItems", nestedArrayItems);

    Decoder decoder = FastSerdeTestsSupport.specificDataAsDecoder(nestedArrayTest, NestedArrayTest.SCHEMA$);

    FastSpecificDatumReader<NestedArrayTest> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(NestedArrayTest.SCHEMA$);
    FastDeserializer<NestedArrayTest> fastDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get();

    NestedArrayTest actual = fastDeserializer.deserialize(decoder);

    List<List<NestedArrayItem>> actualNestedArrayItems =
        (List<List<NestedArrayItem>>) FastSerdeTestsSupport.getField(actual,"NestedArrayItems");
    Assert.assertEquals(1, actualNestedArrayItems.size());
    Assert.assertEquals(1, actualNestedArrayItems.get(0).size());
    Assert.assertEquals("itemName", String.valueOf(FastSerdeTestsSupport.getField(actualNestedArrayItems.get(0).get(0),"ItemName")));
  }
}
