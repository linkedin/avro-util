/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;


public class AvroSchemaUtils {
  public static Set<AvroSchema> schemasToAvroSchemas(Set<Schema> schemas) {
    HashSet<AvroSchema> avroSchemas = new HashSet();
    AvscParser avscParser = new AvscParser();
    for (Schema schema : schemas) {
      // TODO: Is this the  correct AvscGenerationConfig to use?
      String avscText = AvroCompatibilityHelper.toAvsc(schema, AvscGenerationConfig.VANILLA_PRETTY);
      avroSchemas.add(avscParser.parse(avscText).getTopLevelSchema());
    }
    return avroSchemas;
  }
}
