/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;


public class SimpleSchemaSet implements SchemaSet {
  private final Map<SchemaId, Schema> idToSchema;
  private final Map<String, Schema> nameToSchemas;

  public SimpleSchemaSet() {
    this.idToSchema = new HashMap<>();
    this.nameToSchemas = new HashMap<>();
  }

  @Override
  public synchronized int size() {
    return idToSchema.size();
  }

  @Override
  public synchronized Schema getByName(String name) {
    return nameToSchemas.get(name);
  }

  @Override
  public synchronized List<Schema> getAll() {
    return new ArrayList<>(nameToSchemas.values());
  }

  @Override
  public synchronized void add(Schema schema) {
    SchemaId id = SchemaId.forSchema(schema);
    idToSchema.put(id, schema);
    nameToSchemas.put(schema.getFullName(), schema);
  }

  @Override
  public synchronized String toString() {
    StringBuilder builder = new StringBuilder("SchemaSet(");
    for (Map.Entry<String, Schema> entry : nameToSchemas.entrySet()) {
      builder.append(entry.getKey());
      builder.append(" -> ");
      builder.append(entry.getValue());
      builder.append(", ");
    }
    builder.append(")");
    return builder.toString();
  }
}