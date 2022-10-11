/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */


package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SpecificFixedTest {

  @DataProvider
  private Object[][] TestRoundTripSerializationProvider() {
    return new Object[][]{
        {vs14.FixedType.class},
        {vs19.FixedType.class}
    };
  }

  @Test(dataProvider = "TestRoundTripSerializationProvider")
  public <T> void testRoundTripSerialization(Class<T> clazz ) throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    T instance =
        generator.randomSpecific(clazz, RecordGenerationConfig.newConfig().withAvoidNulls(true));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream os = null;
    ObjectInputStream ois = null;
    try {

      os = new ObjectOutputStream(baos);
      os.writeObject(instance);
      os.flush();

      ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
      T deserializedInstance = (T) ois.readObject();

      Assert.assertEquals(deserializedInstance, instance);
    } finally {
      baos.close();
      os.close();
      ois.close();
    }
  }
}
