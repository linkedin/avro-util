/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.linkedin.avroutil1.model.AvroSchema;
import java.util.List;


/**
 * writes {@link com.linkedin.avroutil1.model.AvroSchema}s out to file(s)
 */
interface AvroSchemaWriter {

  List<AvscFile> write(AvroSchema schema, AvscWriterConfig config);

  default AvscFile writeSingle(AvroSchema schema) {
    List<AvscFile> list = write(schema, AvscWriterConfig.CORRECT_MITIGATED);
    if (list == null || list.size() != 1) {
      throw new IllegalStateException("expecting single schema, got " + (list == null ? 0 : list.size()));
    }
    return list.get(0);
  }
}
