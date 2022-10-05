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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;
import vs19.FixedType;


public class FixedTypeTest {

  private byte[] getByteArrayFromBAIS(ByteArrayInputStream bais) throws IOException {
    byte[] array = new byte[bais.available()];
    bais.read(array);

    return array;
  }

  @Test
  public void testRoundTripSerialization() throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();

    vs19.FixedType instance =
        generator.randomSpecific(vs19.FixedType.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));

    //Create output stream
    ByteArrayOutputStream baos = new ByteArrayOutputStream(instance.bytes().length);
    baos.write(instance.bytes(), 0, instance.bytes().length);

    //Create input stream from output stream
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

    //read byte array from input stream
    byte[] fixedTypeBytes = getByteArrayFromBAIS(bais);

    // Should equal byte[] from original instance
    Assert.assertEquals(fixedTypeBytes, instance.bytes());
  }

  @Test
  public void testRoundTripSerializationWithReadWriteExternal() throws IOException, ClassNotFoundException {
    RandomRecordGenerator generator = new RandomRecordGenerator();

    vs19.FixedType instance =
        generator.randomSpecific(vs19.FixedType.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));

    System.err.println(instance instanceof java.io.Serializable);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(baos);
    os.writeObject(instance);

    System.err.println(Arrays.toString(baos.toByteArray()));

    System.err.println(Arrays.toString(
        ((FixedType) new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject()).bytes()));

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    vs19.FixedType deserializedInstance = (FixedType) ois.readObject();

    Assert.assertEquals(deserializedInstance, instance);
  }
}
