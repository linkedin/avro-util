/*
 * Copyright 2025 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import com.linkedin.avroutil1.compatibility.CustomDecoder;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;

/**
 * Extension of {@link SpecificRecordBase} that allows for custom decoding using a custom decoder.
 */
public abstract class SpecificRecordBaseExt extends SpecificRecordBase {

    /**
     * Indicates whether this record has custom decoding support enabled.
     *
     * @return true if custom decoding is enabled, false otherwise
     */
    public boolean isCustomDecodingEnabled() {
        return false;
    }

    /**
     * Custom decode method to be implemented by subclasses for custom decoding logic.
     *
     * @param in the custom decoder to use for decoding
     * @throws IOException if an I/O error occurs during decoding
     */
    public void customDecode(CustomDecoder in) throws IOException {
        throw new UnsupportedOperationException("customDecode not implemented");
    }
}
