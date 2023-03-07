/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.List;
import org.apache.avro.Schema;

/**
 * Builder for creating {@link Schema} instances at runtime
 */
public interface SchemaBuilder {

    /**
     * sets the {@link Schema.Type} of the schema being built
     * @param type a schema type
     * @return the builder
     */
    SchemaBuilder setType(Schema.Type type);

    /**
     * sets the name of a (named) schema being built
     * @param name either a simple of a full ("x.y.Z") name.
     *             name must be simple if {@link #setNamespace(String)} is used
     * @return the builder
     */
    SchemaBuilder setName(String name);

    /**
     * sets the namespace of a (named) schema being built
     * @param namespace namespace
     * @return this builder
     */
    SchemaBuilder setNamespace(String namespace);

    /**
     * sets whether nested named schemas use their ancestor's namespace if the nested schemas do not have namespace
     * @param inheritNamespace true if nested schemas inherit ancestor's namespace, false otherwise
     * @return this builder
     */
    SchemaBuilder setInheritNamespace(boolean inheritNamespace);

    /**
     * sets the element type for the (array) schema being built
     * @param elementSchema schema of array elements
     * @return the builder
     */
    SchemaBuilder setElementType(Schema elementSchema);

    /**
     * sets the value type for the (map) schema being built (keys are always strings in avro)
     * @param valueSchema schema of map values
     * @return the builder
     */
    SchemaBuilder setValueType(Schema valueSchema);

    /**
     * sets the size for the (fixed) schema being built
     * @param size number of bytes in a fixed blob schema
     * @return the builder
     */
    SchemaBuilder setSize(int size);

    /**
     * sets of symbols for the (enum) schema being built
     * @param symbols the (distinct, ordered) symbols
     * @return the builder
     */
    SchemaBuilder setSymbols(List<String> symbols);

    /**
     * sets the default symbol for the (enum) schema being built
     * @param defaultSymbol the default value
     * @return the builder
     */
    SchemaBuilder setDefaultSymbol(String defaultSymbol);

    /**
     * add a field to the (record) schema under construction. field is added
     * to the end of the field list
     * @param field new field to add
     * @return the builder
     * @throws IllegalArgumentException if a field by the same name
     * (case INSENSITIVE) exists
     */
    SchemaBuilder addField(Schema.Field field);

    /**
     * add a field to the (record) schema under construction at the specified position.
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
     * sets the union branches of (union) schema being built
     * @param branches list of union branches
     * @return the builder
     */
    SchemaBuilder setUnionBranches(List<Schema> branches);

    /**
     * constructs a {@link Schema} out of this builder
     * @return a {@link Schema}
     */
    Schema build();
}
