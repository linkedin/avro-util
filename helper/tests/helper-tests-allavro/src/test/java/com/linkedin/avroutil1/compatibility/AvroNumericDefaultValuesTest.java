/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * show vanilla avro's behaviour around default values for numeric fields
 */
public class AvroNumericDefaultValuesTest {

  @Test
  public void testDefaultsForFloatField() throws Exception {

    //show that avro equals() cares about int vs float default values

    String floatDefaultAvsc = "{\"type\": \"record\", \"name\": \"Rec\", \"fields\": [{\"name\": \"f\", \"type\": \"float\", \"default\": 0.0}]}";
    String intDefaultAvsc = "{\"type\": \"record\", \"name\": \"Rec\", \"fields\": [{\"name\": \"f\", \"type\": \"float\", \"default\": 0}]}";

    Schema floatDefaultSchema = Schema.parse(floatDefaultAvsc);
    Schema intDefaultSchema = Schema.parse(intDefaultAvsc);

    boolean oneWay = floatDefaultSchema.equals(intDefaultSchema);
    boolean otherWay = intDefaultSchema.equals(floatDefaultSchema);

    Assert.assertEquals(oneWay, otherWay, "equals() should be symmetric");

    //all versions of avro do not treat int and float defaults as equals()
    AvroVersion avroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertFalse(oneWay, "avro " + avroVersion + " should care about int vs float defaults");
  }
}
