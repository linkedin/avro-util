/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.exception;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilMissingFieldExceptionTest {

  @Test
  public void testAvroUtilMissingFieldExceptionWithCause() throws Exception {
    Throwable throwable = new Throwable("throwable");
    AvroRuntimeException avroRuntimeException = new AvroRuntimeException(throwable);

    AvroUtilMissingFieldException avroUtilMissingFieldException =
        new AvroUtilMissingFieldException(avroRuntimeException);

    Assert.assertNotNull(avroUtilMissingFieldException);

    Assert.assertEquals(avroUtilMissingFieldException.getCause(), avroRuntimeException);
  }

  @Test
  public void testAvroUtilMissingFieldExceptionWithMessage() throws Exception {
    String message = "Avro exception message";
    String fieldName = "field1";

    Schema.Field field = new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), "field doc", null);

    AvroUtilMissingFieldException avroUtilMissingFieldException = new AvroUtilMissingFieldException(message, field);

    Assert.assertNotNull(avroUtilMissingFieldException);

    Assert.assertEquals(avroUtilMissingFieldException.getMessage(), message);

    String expectedPathStr = "Path in schema: --> " + fieldName;

    Assert.assertEquals(avroUtilMissingFieldException.toString(), expectedPathStr);
  }

  @Test
  public void testaddParentField() throws Exception {
    String message = "Avro exception message";

    String fieldName1 = "field1";
    Schema.Field field1 = new Schema.Field(fieldName1, Schema.create(Schema.Type.LONG), "field doc", null);

    AvroUtilMissingFieldException avroUtilMissingFieldException = new AvroUtilMissingFieldException(message, field1);

    Assert.assertNotNull(avroUtilMissingFieldException);

    //add another field
    String fieldName2 = "field2";
    Schema.Field field2 = new Schema.Field(fieldName2, Schema.create(Schema.Type.LONG), "field2 doc", null);

    avroUtilMissingFieldException.addParentField(field2);

    String expectedPathStr = "Path in schema: --> " + fieldName2 + " --> " + fieldName1;

    Assert.assertEquals(avroUtilMissingFieldException.toString(), expectedPathStr);
  }
}
