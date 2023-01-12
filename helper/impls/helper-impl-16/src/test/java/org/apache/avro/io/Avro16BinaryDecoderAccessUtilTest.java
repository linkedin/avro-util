/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro16BinaryDecoderAccessUtilTest {
  private static final byte[] BYTES = new byte[10];
  private static final int OFFSET = 0;
  private static final int LENGTH = 10;

  @Test
  public void testNewBinaryDecoderReuse() throws Exception {
    BinaryDecoder mockedBinaryDecoder = Mockito.mock(BinaryDecoder.class);
    BinaryDecoder configuredBinaryDecoder = Mockito.mock(BinaryDecoder.class);

    Mockito.when(mockedBinaryDecoder.configure(BYTES, OFFSET, LENGTH)).thenReturn(configuredBinaryDecoder);

    BinaryDecoder returnedBinaryDecoder =
        Avro16BinaryDecoderAccessUtil.newBinaryDecoder(BYTES, OFFSET, LENGTH, mockedBinaryDecoder);

    Mockito.verify(mockedBinaryDecoder, Mockito.times(1)).configure(BYTES, OFFSET, LENGTH);

    Assert.assertEquals(returnedBinaryDecoder, configuredBinaryDecoder,
        "Verify the configured BinaryDecoder is returned");
  }

  @Test
  public void testNewBinaryDecoderNotReuse() throws Exception {
    Map<BinaryDecoder, List<Object>> constructorArgs = new HashMap<>();

    try (MockedConstruction<BinaryDecoder> mockedBinaryDecoder = Mockito.mockConstruction(BinaryDecoder.class,
        (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments())))) {

      BinaryDecoder returnedBinaryDecoder = Avro16BinaryDecoderAccessUtil.newBinaryDecoder(BYTES, OFFSET, LENGTH, null);

      Assert.assertEquals(mockedBinaryDecoder.constructed().size(), 1, "Verify one BinaryDecoder is created");

      BinaryDecoder createdBinaryDecoder = mockedBinaryDecoder.constructed().get(0);

      Assert.assertEquals(returnedBinaryDecoder, createdBinaryDecoder,
          "Verify the new created BinaryDecoder is returned");

      //Verify parameters are correctly used when creating the new BinaryDecoder
      List<Object> parameters = constructorArgs.get(createdBinaryDecoder);
      Assert.assertEquals(parameters.size(), 3, "Verify the number of parameter is correct");
      Assert.assertEquals(parameters.get(0), BYTES, "Verify first parameter");
      Assert.assertEquals(parameters.get(1), OFFSET, "Verify first parameter");
      Assert.assertEquals(parameters.get(2), LENGTH, "Verify first parameter");
    }
  }
}
