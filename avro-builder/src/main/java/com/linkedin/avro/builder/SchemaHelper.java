/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.builder;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;


public class SchemaHelper {

  private SchemaHelper() { }

  public static Schema parse(String json, Schema... includes) {
    return parse(json, Arrays.asList(includes));
  }

  @Deprecated //here to preserve old method signature
  public static Schema parse(String json, List<Schema> includes) {
    return parse(json, (Collection<Schema>) includes);
  }

  public static Schema parse(String json, Collection<Schema> includes) {
    SchemaParseResult result = AvroCompatibilityHelper.parse(json, SchemaParseConfiguration.STRICT, includes);
    return result.getMainSchema();
  }
}