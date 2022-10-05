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
import org.testng.annotations.Test;
import vs19.FixedType;


public class FixedTypeTest {

  @Test
  public void testRoundTripSerialization() throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();

    vs19.FixedType instance =
        generator.randomSpecific(vs19.FixedType.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream os = null;
    ObjectInputStream ois = null;
    try {

      os = new ObjectOutputStream(baos);
      os.writeObject(instance);
      os.flush();

      ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
      vs19.FixedType deserializedInstance = (FixedType) ois.readObject();

      Assert.assertEquals(deserializedInstance, instance);
    } finally {
      baos.close();
      os.close();
      ois.close();
    }
  }
}
