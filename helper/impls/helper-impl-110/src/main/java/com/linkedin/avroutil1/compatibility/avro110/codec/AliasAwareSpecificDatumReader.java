/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110.codec;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * an extension of {@link SpecificDatumReader} that, upon failing to look up a class
 * by its fullname (FQCN), also tries fishing for it by aliases. <br>
 *
 * this sort of crap is required when dealing with specific record classes that were generated
 * by avro 1.4 (see AVRO-702)
 *
 * @param <T>
 */
public class AliasAwareSpecificDatumReader<T> extends SpecificDatumReader<T> {

    public AliasAwareSpecificDatumReader() {
        this(null, null);
    }

    public AliasAwareSpecificDatumReader(Class<T> c) {
        this(new AliasAwareSpecificData(c.getClassLoader()));
        setSchema(this.getSpecificData().getSchema(c));
    }

    public AliasAwareSpecificDatumReader(Schema schema) {
        this(schema, schema);
    }

    public AliasAwareSpecificDatumReader(Schema writer, Schema reader) {
        super(writer, reader, new AliasAwareSpecificData());
    }

    public AliasAwareSpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
        throw new UnsupportedOperationException("providing custom SpecificData not supported (yet?)");
    }

    protected AliasAwareSpecificDatumReader(SpecificData data) {
        throw new UnsupportedOperationException("providing custom SpecificData not supported (yet?)");
    }
}
