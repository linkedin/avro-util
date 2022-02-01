/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StringTypePropTest {

    @Test
    public void testSchemaStringPropImpact() throws Exception {
        //shows that the "avro.java.String" property has an effect on generic records returned from deserialization

        String avscWith = TestUtil.load("RecordWithStringType-withProp.avsc");
        String avscWithout = TestUtil.load("RecordWithStringType-withoutProp.avsc");
        Schema schemaWith = Schema.parse(avscWith);
        Schema schemaWithout = Schema.parse(avscWithout);

        RandomRecordGenerator gen = new RandomRecordGenerator();
        IndexedRecord originalRecord = (IndexedRecord) gen.randomGeneric(schemaWith);
        byte[] serialized = AvroCodecUtil.serializeBinary(originalRecord);

        GenericRecord deserializedWith = AvroCodecUtil.deserializeAsGeneric(serialized, schemaWith, schemaWith);
        GenericRecord deserializedWithout = AvroCodecUtil.deserializeAsGeneric(serialized, schemaWithout, schemaWithout);

        Object value1 = deserializedWith.get("strProp");
        Object value2 = deserializedWithout.get("strProp");

        AvroVersion avroVer = AvroCompatibilityHelper.getRuntimeAvroVersion();
        Assert.assertTrue(value2 instanceof Utf8, "value without prop is " + value2.getClass() + " under " + avroVer);
        switch (avroVer) {
            case AVRO_1_4:
            case AVRO_1_5:
                Assert.assertTrue(value1 instanceof Utf8, "value with prop is " + value1.getClass() + " under " + avroVer);
                break;
            default:
                Assert.assertTrue(value1 instanceof java.lang.String, "value with prop is " + value1.getClass() + " under " + avroVer);
        }
    }
}
