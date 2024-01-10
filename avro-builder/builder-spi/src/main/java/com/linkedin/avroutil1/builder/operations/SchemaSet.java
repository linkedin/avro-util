/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations;


import java.util.List;

import org.apache.avro.Schema;


/**
 * provides a set of schemas
 */
public interface SchemaSet {

  /**
   * @return how many schemas in the set
   */
  int size();

  /**
   * @param name FQCN of a schema (so "com.acme.Foo")
   * @return the schema, if this set has it
   */
  Schema getByName(String name);

  /**
   * @return all schemas in this set
   */
  List<Schema> getAll();

  /**
   * @param schema to be added
   */
  void add(Schema schema);
}