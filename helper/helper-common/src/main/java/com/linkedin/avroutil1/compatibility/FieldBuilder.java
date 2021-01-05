/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;


/**
 * an builder interface to build a Schema.Field with given parameters.
 * The defaultValue needed to be defined in implemented classes since default
 * value type is changed to Object since AVRO 1.9
 */
public interface FieldBuilder {

  FieldBuilder setField(Schema.Field field);

  FieldBuilder setSchema(Schema schema);

  FieldBuilder setDoc(String doc);

  FieldBuilder setOrder(Order order);

  FieldBuilder copyFromField(Schema.Field field);

  Schema.Field build();
}
