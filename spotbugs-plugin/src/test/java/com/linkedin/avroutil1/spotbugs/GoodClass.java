/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.InputStream;
import java.io.OutputStream;

@SuppressWarnings("unused") //not used in code, but the compiled bytecode is used in tests
public class GoodClass {

    public void instantiateBinaryDecoder() {
        BinaryDecoder bobTheDecoder = AvroCompatibilityHelper.newBinaryDecoder( (InputStream) null);
        BinaryDecoder robertTheDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[] {1, 2, 3}, null);
    }

    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = AvroCompatibilityHelper.newBinaryEncoder( (OutputStream) null);
    }

    public void instantiateCompatibleJsonDecoder() throws Exception {
        Decoder bobTheDecoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(null, "bob");
    }

    public void instantiateJsonEncoder() throws Exception {
        Encoder bobTheEncoder = AvroCompatibilityHelper.newJsonEncoder(null, null, true, AvroVersion.AVRO_1_4);
    }

    public void instanceOfGenericRecord() throws Exception {
        SpecificRecordBase someRecord = null;
        //noinspection ConstantConditions
        if (AvroCompatibilityHelper.isGenericRecord(someRecord)) {
            System.err.println("boom");
        }
    }

    public void instantiateEnumSymbol() {
        AvroCompatibilityHelper.newEnumSymbol(null, "bob");
    }

    public void instantiationFixed() throws Exception {
        AvroCompatibilityHelper.newFixed(null, new byte[] {1, 2, 3});
    }

    public void instantiateSchemaField() throws Exception {
        AvroCompatibilityHelper.createSchemaField("file", null, "doc", null, Schema.Field.Order.ASCENDING);
    }
}
