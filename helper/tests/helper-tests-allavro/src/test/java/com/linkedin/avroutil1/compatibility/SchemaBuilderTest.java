/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaBuilderTest {

    @Test
    public void testSchemaPropSupportOnClone() throws Exception {
        AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
        String avsc = TestUtil.load("RecordWithFieldProps.avsc");
        Schema originalSchema = Schema.parse(avsc);

        SchemaBuilder builder = AvroCompatibilityHelper.cloneSchema(originalSchema);
        Schema newSchema = builder.build();

        Assert.assertNotSame(newSchema, originalSchema);
        Assert.assertEquals(newSchema.getFields().size(), 1); //got fields
        Assert.assertNull(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "noSuchSchemaProp"));
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaStringProp"), "\"stringyMcStringface\"");
        if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_7)) {
            //sadly avro 1.4/5/6 do not preserve any other (non textual) props
            return;
        }
        String intPropAsJsonLiteral = AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaIntProp");
        if (runtimeAvroVersion.equals(AvroVersion.AVRO_1_7) && intPropAsJsonLiteral == null) {
            //we must be under avro < 1.7.3. avro 1.7 only supports non-text props at 1.7.3+
            return;
        }
        Assert.assertEquals(intPropAsJsonLiteral, "24");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaFloatProp"), "1.2");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaNullProp"), "null");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaBoolProp"), "false");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaObjectProp"), "{\"e\":\"f\",\"g\":\"h\"}");
    }
}
