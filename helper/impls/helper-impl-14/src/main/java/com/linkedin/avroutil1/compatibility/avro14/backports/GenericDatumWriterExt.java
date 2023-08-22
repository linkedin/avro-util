/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14.backports;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;


/**
 * this class allows constructing a {@link GenericDatumWriter} with
 * a specified {@link GenericData} instance under avro 1.4
 * @param <T>
 */
public class GenericDatumWriterExt<T> extends GenericDatumWriter<T> {

  public GenericDatumWriterExt(Schema root, GenericData genericData) {
    super(root, genericData);
  }
}
