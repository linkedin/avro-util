/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17.backports;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;


/**
 * this class allows constructing a {@link SpecificDatumWriter} with
 * a specified {@link SpecificData} instance under avro 1.7
 * <br>
 * NOTE - the relevant constructor was made public only mid-1.7
 * @param <T>
 */
public class SpecificDatumWriterExt<T> extends SpecificDatumWriter<T> {
  public SpecificDatumWriterExt(Schema root, SpecificData specificData) {
    super(root, specificData);
  }
}
