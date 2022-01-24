/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

/**
 * Builder for creating {@link Schema} instances at runtime
 */
public interface SchemaBuilder {

    /**
     * sets the {@link Schema.Type} of the schema being built
     * @param type a schema type
     */
    SchemaBuilder setType(Schema.Type type);

    /**
     * add a field to the schema under construction. field is added
     * to the end of the field list
     * @param field new field to add
     * @return the builder
     * @throws IllegalArgumentException if a field by the same name
     * (case INSENSITIVE) exists
     */
    SchemaBuilder addField(Schema.Field field);

    /**
     * add a field to the schema under construction at the specified position.
     * existing fields starting from the specified position are "right shifted"
     * @param position desired position for the field to be added, 0 based.
     * @param field field to add
     * @return the builder
     * @throws IllegalArgumentException if a field by the same name
     * (case INSENSITIVE) exists
     * @throws IndexOutOfBoundsException if index is invalid
     */
    SchemaBuilder addField(int position, Schema.Field field);

    /**
     * removes a field by its (case INSENSITIVE) name, if such a field exists.
     * @param fieldName name of field to be removed. required.
     * @return the builder
     * @throws IllegalArgumentException if argument is null or emoty
     */
    SchemaBuilder removeField(String fieldName);

    /**
     * removes a field by its position.
     * @param position position (0 based) os the field to remove
     * @return the builder
     * @throws IndexOutOfBoundsException if position is invalid
     */
    SchemaBuilder removeField(int position);

    /**
     * constructs a {@link Schema} out of this builder
     * @return a {@link Schema}
     */
    Schema build();
}
