/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15.backports;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;


/**
 * this class allows constructing a {@link GenericDatumReader} with
 * a specified {@link GenericData} instance under avro 1.5
 * @param <T>
 */
public class GenericDatumReaderExt<T> extends GenericDatumReader<T> {

  public GenericDatumReaderExt(Schema writer, Schema reader, GenericData genericData) {
    super(writer, reader, genericData);
  }
}
