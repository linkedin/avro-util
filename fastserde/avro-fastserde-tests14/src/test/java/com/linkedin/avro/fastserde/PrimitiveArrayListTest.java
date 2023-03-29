package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.primitive.PrimitiveFloatArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PrimitiveArrayListTest {
  @Test
  public void testPrimitiveLongArrayAdd() {
    List<Long> newVector = new PrimitiveLongArrayList(1);
    newVector.add(0, 1L);
    newVector.add(1, 2L);
    newVector.add(2, 3L);
    List<Long> expectedVector = Arrays.asList(1L, 2L, 3L);
    Assert.assertEquals(newVector, expectedVector);
  }
  @Test
  public void testPrimitiveFloatArrayAdd() {
    List<Float> newVector = new PrimitiveFloatArrayList(1);
    newVector.add(0, 1.0f);
    newVector.add(1, 2.0f);
    newVector.add(2, 3.0f);
    List<Float> expectedVector = Arrays.asList(1.0f, 2.0f, 3.0f);
    Assert.assertEquals(newVector, expectedVector);
  }
}