/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro111FieldBuilderTest {
  @Test
  public void testArrayOfEnumDefaultValue() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("FieldWithArrayOfEnumDefaultValue.avsc"));
    Schema.Field field = schema.getField("arrayOfEnum");
    Object defaultValue = AvroCompatibilityHelper.getGenericDefaultValue(field);
    FieldBuilder builder = AvroCompatibilityHelper.newField(field);
    builder.setDefault(defaultValue);

    // Test that .build() should not throw an exception.
    Schema.Field resField = builder.build();
    Assert.assertNotNull(resField);
  }
}
