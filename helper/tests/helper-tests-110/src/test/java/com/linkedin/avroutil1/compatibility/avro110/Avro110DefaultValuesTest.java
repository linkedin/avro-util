/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro110DefaultValuesTest {

  @Test
  public void demonstrateDefaultValueBehaviour() throws Exception {
    Schema schema = by14.HasComplexDefaults.SCHEMA$;
    Schema.Field field = schema.getField("fieldWithDefaultRecord");

    Object specificDefault = SpecificData.get().getDefaultValue(field);
    Assert.assertNotNull(specificDefault);
    Assert.assertTrue(specificDefault instanceof by14.DefaultRecord);
    Object genericDefault = GenericData.get().getDefaultValue(field);
    Assert.assertNotNull(genericDefault);
    Assert.assertTrue(genericDefault instanceof GenericData.Record);
  }
}
