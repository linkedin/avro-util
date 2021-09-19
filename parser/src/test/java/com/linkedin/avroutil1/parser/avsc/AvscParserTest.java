/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.exceptions.AvroSyntaxException;
import com.linkedin.avroutil1.parser.exceptions.JsonParseException;
import com.linkedin.avroutil1.testcommon.TestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class AvscParserTest {

    @Test
    public void testParseInvalidJson() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidJsonRecord.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof JsonParseException);
    }

    @Test
    public void testParseInvalidType() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidTypeRecord.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testParseInvalidType2() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidTypeRecord2.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testParseInvalidDoc() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidDocRecord.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testParseInvalidField() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidFieldRecord.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testParseInvalidEnumSymbol() throws Exception {
        String avsc = TestUtil.load("schemas/TestInvalidEnumSymbol.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testParseBadEnumDefault() throws Exception {
        String avsc = TestUtil.load("schemas/TestBadEnumDefault.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNotNull(result.getParseError());
        Assert.assertTrue(result.getParseError() instanceof AvroSyntaxException);
    }

    @Test
    public void testSimpleParse() throws Exception {
        String avsc = TestUtil.load("schemas/TestRecord.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNull(result.getParseError());
        AvroSchema schema = result.getTopLevelSchema();
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.type(), AvroType.RECORD);
        AvroRecordSchema recordSchema = (AvroRecordSchema) schema;
        Assert.assertEquals(recordSchema.getFullName(), "com.acme.TestRecord");
        List<AvroSchemaField> fields = recordSchema.getFields();
        Assert.assertNotNull(fields);
        Assert.assertEquals(fields.size(), 9);

        Assert.assertEquals(fields.get(0).getPosition(), 0);
        Assert.assertEquals(fields.get(0).getName(), "booleanField");
        Assert.assertEquals(fields.get(0).getSchema().type(), AvroType.BOOLEAN);

        Assert.assertEquals(fields.get(1).getPosition(), 1);
        Assert.assertEquals(fields.get(1).getName(), "intField");
        Assert.assertEquals(fields.get(1).getSchema().type(), AvroType.INT);

        Assert.assertEquals(fields.get(2).getPosition(), 2);
        Assert.assertEquals(fields.get(2).getName(), "longField");
        Assert.assertEquals(fields.get(2).getSchema().type(), AvroType.LONG);

        Assert.assertEquals(fields.get(3).getPosition(), 3);
        Assert.assertEquals(fields.get(3).getName(), "floatField");
        Assert.assertEquals(fields.get(3).getSchema().type(), AvroType.FLOAT);

        Assert.assertEquals(fields.get(4).getPosition(), 4);
        Assert.assertEquals(fields.get(4).getName(), "doubleField");
        Assert.assertEquals(fields.get(4).getSchema().type(), AvroType.DOUBLE);

        Assert.assertEquals(fields.get(5).getPosition(), 5);
        Assert.assertEquals(fields.get(5).getName(), "bytesField");
        Assert.assertEquals(fields.get(5).getSchema().type(), AvroType.BYTES);

        Assert.assertEquals(fields.get(6).getPosition(), 6);
        Assert.assertEquals(fields.get(6).getName(), "stringField");
        Assert.assertEquals(fields.get(6).getSchema().type(), AvroType.STRING);

        Assert.assertEquals(fields.get(7).getPosition(), 7);
        Assert.assertEquals(fields.get(7).getName(), "enumField");
        Assert.assertEquals(fields.get(7).getSchema().type(), AvroType.ENUM);
        Assert.assertEquals(((AvroEnumSchema)fields.get(7).getSchema()).getFullName(), "innerNamespace.SimpleEnum");
        Assert.assertEquals(((AvroEnumSchema)fields.get(7).getSchema()).getSymbols(), Arrays.asList("A", "B", "C"));

        Assert.assertEquals(fields.get(8).getPosition(), 8);
        Assert.assertEquals(fields.get(8).getName(), "fixedField");
        Assert.assertEquals(fields.get(8).getSchema().type(), AvroType.FIXED);
        Assert.assertEquals(((AvroFixedSchema)fields.get(8).getSchema()).getFullName(), "com.acme.SimpleFixed");
        Assert.assertEquals(((AvroFixedSchema)fields.get(8).getSchema()).getSize(), 7);
    }

    @Test
    public void testSelfReference() throws Exception {
        String avsc = TestUtil.load("schemas/LongList.avsc");
        AvscParser parser = new AvscParser();
        AvscParseResult result = parser.parse(avsc);
        Assert.assertNull(result.getParseError());
        AvroRecordSchema schema = (AvroRecordSchema) result.getTopLevelSchema();

        //schema.next[1] == schema

        AvroSchemaField nextField = schema.getField("next");
        AvroUnionSchema union = (AvroUnionSchema) nextField.getSchema();
        SchemaOrRef secondBranch = union.getTypes().get(1);
        Assert.assertSame(secondBranch.getSchema(), schema);
    }
}
