/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public abstract class BadClass {

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

//    //only compiles under avro 1.5+
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

    public void instanceOfGenericRecord() throws Exception {
        SpecificRecordBase someRecord = null;
        //noinspection ConstantConditions
        if (someRecord instanceof GenericRecord) {
            System.err.println("boom");
        }
    }

    //compiles under avro < 1.6
    public static class OldSchemaConstructable implements SpecificDatumReader.SchemaConstructable {
        private final Schema schema;

        public OldSchemaConstructable(Schema schema) {
            this.schema = schema;
        }
    }

    //compiles under avro < 1.6
    public SpecificDatumReader.SchemaConstructable[] useOldSchemaConstructable(
            SpecificDatumReader.SchemaConstructable arg1,
            List<SpecificDatumReader.SchemaConstructable> arg2
    ) {
        SpecificDatumReader.SchemaConstructable constructableA = new OldSchemaConstructable(Schema.parse("bla"));
        @SuppressWarnings("ConstantConditions")
        SpecificDatumReader.SchemaConstructable constructableB = (SpecificDatumReader.SchemaConstructable) new Object();
        return null;
    }

    //compiles under avro < 1.6
    public abstract SpecificDatumReader.SchemaConstructable[] useOldSchemaConstructableSomeMore (
            SpecificDatumReader.SchemaConstructable argA
    );

//    //only compiles under avro 1.6+
//    public void callSpecificDataNewInstance() {
//        SpecificData.newInstance(ArrayList.class, null);
//    }
}
