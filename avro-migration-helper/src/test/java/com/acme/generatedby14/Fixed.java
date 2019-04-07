/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.acme.generatedby14;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;


/**
 * this demonstrates the output of generating a fixed-type class would be under avro 1.4 after
 * transformations done in Avro14Adapter
 */
@SuppressWarnings("all")
@org.apache.avro.specific.FixedSize(16)
public class Fixed extends org.apache.avro.specific.SpecificFixed {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"fixed\",\"name\":\"Fixed\",\"size\":16,\"namespace\":\"com.acme.generatedby14\"}");

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  private static final org.apache.avro.io.DatumWriter WRITER$ = new org.apache.avro.specific.SpecificDatumWriter<Fixed>(SCHEMA$);

  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, AvroCompatibilityHelper.newBinaryEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader READER$ = new org.apache.avro.specific.SpecificDatumReader<Fixed>(SCHEMA$);

  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, AvroCompatibilityHelper.newBinaryDecoder(in));
  }
}