/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

@SuppressWarnings("unused") //not used in code, but the compiled bytecode is used in tests
public abstract class BadClass {

    //compiles under avro < 1.5
    public void instantiateBinaryDecoder() {
        BinaryDecoder bobTheDecoder = new BinaryDecoder(null);
    }

    //compiles under avro < 1.5
    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = new BinaryEncoder(null);
    }

    //only compiles under avro < 1.5
    public void instantiateJsonDecoder() throws Exception {
        JsonDecoder bobTheDecoder = new JsonDecoder(null, "bob");
        JsonDecoder robertTheDecoder = new JsonDecoder(null, (InputStream) null);
    }

//    //only compiles under avro 1.5+
//    public void instantiateJsonDecoderViaFactory() throws Exception {
//        JsonDecoder bobTheDecoder = DecoderFactory.get().jsonDecoder(null, "bob");
//    }

    //only compiles under avro < 1.5
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

    //compiles under avro < 1.5
    public void instantiateEnumSymbol() {
        new GenericData.EnumSymbol("bob");
    }

    //compiles under avro < 1.5
    public void fixedInstantiation() throws Exception {
        new GenericData.Fixed((Schema) null);
        new GenericData.Fixed(new byte[] {1, 2, 3});
    }

    //compiles under avro < 1.9
    public void schemaFieldInstantiation() throws Exception {
        new Schema.Field("file", null, "doc", null, Schema.Field.Order.ASCENDING);
    }

//    //compiles under avro 1.7
//    public void propAccessUnder17() throws Exception {
//        Schema s = Schema.parse("whatever");
//        Schema.Field f = s.getField("f");
//        org.apache.avro.JsonProperties p = (org.apache.avro.JsonProperties) Schema.parse("something else");
//
//        //12 violations follow
//
//        s.getProps();
//        f.getProps();
//        p.getProps();
//
//        s.addProp("K", (org.codehaus.jackson.JsonNode) null);
//        f.addProp("K", (org.codehaus.jackson.JsonNode) null);
//        p.addProp("K", (org.codehaus.jackson.JsonNode) null);
//
//        s.getJsonProp("K");
//        f.getJsonProp("K");
//        p.getJsonProp("K");
//
//        s.getJsonProps();
//        f.getJsonProps();
//        p.getJsonProps();
//    }

//    //compiles under avro 1.9
//    public void propAccessUnder19() throws Exception {
//        Schema s = Schema.parse("whatever");
//        Schema.Field f = s.getField("f");
//        org.apache.avro.JsonProperties p = (org.apache.avro.JsonProperties) Schema.parse("something else");
//
//        //12 violations follow
//
//        s.addProp("K", new Object());
//        f.addProp("K", new Object());
//        p.addProp("K", new Object());
//
//        s.getObjectProp("K");
//        f.getObjectProp("K");
//        p.getObjectProp("K");
//
//        s.getObjectProps();
//        f.getObjectProps();
//        p.getObjectProps();
//
//        s.addAllProps(null);
//        f.addAllProps(null);
//        p.addAllProps(null);
//    }

//    //compiles under avro 1.6+
//    public void genericRecordBuilderUsage(GenericRecordBuilder arg) throws Exception {
//        String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
//        Schema schema = Schema.parse(avsc);
//        Object var = new GenericRecordBuilder(schema);
//        //noinspection ConstantConditions
//        if (var instanceof GenericRecordBuilder) {
//            @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
//            GenericRecordBuilder grb = (GenericRecordBuilder) var;
//            grb.build();
//        }
//    }

//    //compiles under avro 1.8+
//    public void binaryMessageEncoderUsage() throws Exception {
//        String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
//        Schema schema = Schema.parse(avsc);
//        BinaryMessageEncoder<IndexedRecord> encoder = new BinaryMessageEncoder<>(GenericData.get(), schema);
//        @SuppressWarnings({"RedundantCast", "rawtypes"})
//        Object var = (BinaryMessageEncoder) null;
//        //noinspection ConstantConditions
//        if (var instanceof BinaryMessageEncoder) {
//            @SuppressWarnings("rawtypes")
//            BinaryMessageEncoder cast = (BinaryMessageEncoder) var;
//        }
//        encoder.encode(null);
//    }

    public void singleSchemaDecoding() throws Exception {
        String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
        Schema schema = Schema.parse(avsc);

        GenericDatumReader<IndexedRecord> genericDatumReader = new GenericDatumReader<>(schema);
        SpecificDatumReader<IndexedRecord> specificDatumReader = new SpecificDatumReader<>(schema);
        //cant test for fastavro because not a dependency of this module
    }
}
