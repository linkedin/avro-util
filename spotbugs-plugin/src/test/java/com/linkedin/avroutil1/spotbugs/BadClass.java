/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;

import java.io.InputStream;
import java.io.OutputStream;

public class BadClass {

    public void instantiateBinaryDecoder() {
        BinaryDecoder bobTheDecoder = new BinaryDecoder(null);
    }

    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = new BinaryEncoder(null);
    }

    public void instantiateJsonDecoder() throws Exception {
        JsonDecoder bobTheDecoder = new JsonDecoder(null, "bob");
        JsonDecoder robertTheDecoder = new JsonDecoder(null, (InputStream) null);
    }

    //only compiles under avro 1.5+
//    public void instantiateJsonDecoderViaFactory() throws Exception {
//        JsonDecoder bobTheDecoder = DecoderFactory.get().jsonDecoder(null, "bob");
//    }

    public void instantiateJsonEncoder() throws Exception {
        JsonEncoder bobTheEncoder = new JsonEncoder(null, (OutputStream) null);
    }

    public void fieldDefaultValueAccess() throws Exception {
        String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
        Schema schema = Schema.parse(avsc);
        Schema.Field field = schema.getField("stringField");
        field.defaultValue();
    }
}
