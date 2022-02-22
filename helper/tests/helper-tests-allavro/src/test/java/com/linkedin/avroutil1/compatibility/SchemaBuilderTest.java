/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaBuilderTest {

    @Test
    public void testSchemaPropSupportOnClone() throws Exception {
        AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
        String avsc = TestUtil.load("RecordWithFieldProps.avsc");
        Schema originalSchema = Schema.parse(avsc);
        Assert.assertEquals(originalSchema.getFields().size(), 2); //got 2 fields

        SchemaBuilder builder = AvroCompatibilityHelper.cloneSchema(originalSchema);
        Schema newSchema = builder.removeField("intField").build();

        Assert.assertNotSame(newSchema, originalSchema);
        Assert.assertEquals(newSchema.getFields().size(), 1); //got 1 field
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

        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaNestedJsonProp"), "\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\"");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaNestedJsonProp", false, false), "{\\\"innerKey\\\" : \\\"innerValue\\\"}");
        Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(newSchema, "schemaNestedJsonProp", false, true), "{\"innerKey\" : \"innerValue\"}");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testTypeRequired() throws Exception {
        AvroCompatibilityHelper.newSchema(null).build();
    }

    @Test
    public void testInvalidNames() throws Exception {
        List<String> symbols = Arrays.asList("A", "B");
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).setName(null).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).setName(".").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).setName(" illegal").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).setName("full.name").setNamespace("a").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(symbols).setName("legal").setNamespace("not legal").build(), IllegalArgumentException.class);

        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName("legal").setNamespace("not legal").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName(null).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName(".").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName(" illegal").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName("full.name").setNamespace("a").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.FIXED).setSize(3).setName("legal").setNamespace("not legal").build(), IllegalArgumentException.class);

        Schema intSchema = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.INT).build();
        Schema.Field f = AvroCompatibilityHelper.newField(null).setName("f").setSchema(intSchema).build();
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName("legal").setNamespace("not legal").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName(null).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName(".").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName(" illegal").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName("full.name").setNamespace("a").build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).addField(f).setName("legal").setNamespace("not legal").build(), IllegalArgumentException.class);
    }

    @Test
    public void testInvalidEnumSymbols() throws Exception {
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(null).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(Arrays.asList("A", null)).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(Arrays.asList("", "A")).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(Arrays.asList("A", "A", "B")).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setSymbols(Arrays.asList("A", "B")).setDefaultSymbol("C").build(), IllegalArgumentException.class);
    }

    @Test
    public void testEnumSchema() throws Exception {
        Schema enumSchema = AvroCompatibilityHelper.newSchema(null)
            .setType(Schema.Type.ENUM)
            .setName("Bob")
            .setSymbols(Arrays.asList("A", "B", "C"))
            .setDefaultSymbol("B")
            .build();
        Assert.assertNotNull(enumSchema);
        Assert.assertEquals(enumSchema.getType(), Schema.Type.ENUM);
        Assert.assertEquals(enumSchema.getFullName(), "Bob");
        Assert.assertEquals(enumSchema.getEnumSymbols(), Arrays.asList("A", "B", "C"));
        String enumDefault = AvroCompatibilityHelper.getEnumDefault(enumSchema);
        Assert.assertEquals(enumDefault, "B");
    }

    @Test
    public void testCollectionSchemas() throws Exception {
        Schema stringSchema = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.STRING).build();
        Assert.assertNotNull(stringSchema);
        Assert.assertEquals(stringSchema.getType(), Schema.Type.STRING);

        Schema arraySchema = AvroCompatibilityHelper.newSchema(null)
            .setType(Schema.Type.ARRAY)
            .setElementType(stringSchema)
            .build();
        Assert.assertNotNull(arraySchema);
        Assert.assertEquals(arraySchema.getType(), Schema.Type.ARRAY);
        Assert.assertEquals(arraySchema.getElementType(),  stringSchema);

        //Map<String, List<String>>
        Schema mapSchema = AvroCompatibilityHelper.newSchema(null)
            .setType(Schema.Type.MAP)
            .setValueType(arraySchema)
            .build();
        Assert.assertNotNull(mapSchema);
        Assert.assertEquals(mapSchema.getType(), Schema.Type.MAP);
        Assert.assertEquals(mapSchema.getValueType(),  arraySchema);
    }

    @Test
    public void testInvalidUnions() throws Exception {
        Schema intSchema1 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.INT).build();
        Schema stringSchema1 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.STRING).build();
        Schema stringSchema2 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.STRING).build();
        Schema recordSchema1 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).setName("a.Rec").addField(
            AvroCompatibilityHelper.newField(null).setName("f").setSchema(intSchema1).build()
        ).build();
        Schema recordSchema2 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.RECORD).setName("b.Rec").addField(
            AvroCompatibilityHelper.newField(null).setName("f").setSchema(intSchema1).build()
        ).build();
        Schema enumSchema1 = AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.ENUM).setName("a.Rec")
            .setSymbols(Arrays.asList("A", "B")).build();

        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION)
            .setUnionBranches(Collections.emptyList()).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION)
            .setUnionBranches(Arrays.asList(null, stringSchema1)).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION)
            .setUnionBranches(Arrays.asList(stringSchema1, stringSchema2)).build(), IllegalArgumentException.class);
        TestUtil.expect(() -> AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION)
            .setUnionBranches(Arrays.asList(recordSchema1, enumSchema1)).build(), IllegalArgumentException.class);
        //this is fine
        AvroCompatibilityHelper.newSchema(null).setType(Schema.Type.UNION)
            .setUnionBranches(Arrays.asList(recordSchema1, recordSchema2)).build();
    }
}
