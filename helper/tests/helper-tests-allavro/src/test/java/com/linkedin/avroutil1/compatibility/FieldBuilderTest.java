/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldBuilderTest {

    @Test
    public void testFieldPropSupportOnClone() throws Exception {
        AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
        String avsc = TestUtil.load("RecordWithFieldProps.avsc");
        Schema schema = Schema.parse(avsc);
        Schema.Field originalField = schema.getField("stringField");

        FieldBuilder builder = AvroCompatibilityHelper.cloneSchemaField(originalField);
        Schema.Field newField = builder.build();

        Assert.assertNotSame(newField, originalField);
        Assert.assertNull(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "noSuchProp"));
        Assert.assertEquals(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "stringProp"), "\"stringValue\"");
        if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_7)) {
            //sadly avro 1.4/5/6 do not preserve any other (non textual) props
            return;
        }
        String intPropAsJsonLiteral = AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "intProp");
        if (runtimeAvroVersion.equals(AvroVersion.AVRO_1_7) && intPropAsJsonLiteral == null) {
            //we must be under avro < 1.7.3. avro 1.7 only supports non-text props at 1.7.3+
            return;
        }
        Assert.assertEquals(intPropAsJsonLiteral, "42");
        Assert.assertEquals(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "floatProp"), "42.42");
        Assert.assertEquals(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "nullProp"), "null");
        Assert.assertEquals(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "boolProp"), "true");
        Assert.assertEquals(AvroCompatibilityHelper.getFieldPropAsJsonString(newField, "objectProp"), "{\"a\":\"b\",\"c\":\"d\"}");
    }
}
