/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15.backports;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * this class allows constructing a {@link SpecificDatumReader} with
 * a specified {@link SpecificData} instance under avro 1.5
 * @param <T>
 */
public class SpecificDatumReaderExt<T> extends SpecificDatumReader<T> {
  public SpecificDatumReaderExt(Schema writer, Schema reader, SpecificData specificData) {
    super(writer, reader, specificData);
  }
}
