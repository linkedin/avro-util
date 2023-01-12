/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.exception;

import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilExceptionTest {

  @Test
  public void testAvroUtilExceptionCreatWithCause() throws Exception {
    Exception cause = new Exception("cause");

    AvroUtilException avroUtilException = new AvroUtilException(cause);

    Assert.assertNotNull(avroUtilException);

    Assert.assertEquals(avroUtilException.getCause(), cause);
  }

  @Test
  public void testAvroUtilExceptionCreatWithMessage() throws Exception {
    String message = "Avro exception message";

    AvroUtilException avroUtilException = new AvroUtilException(message);

    Assert.assertNotNull(avroUtilException);

    Assert.assertEquals(avroUtilException.getMessage(), message);
  }
}