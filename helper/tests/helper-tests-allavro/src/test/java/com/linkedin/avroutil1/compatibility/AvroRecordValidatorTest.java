/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;
import under14.RecordWithSelfReferences;

import java.util.List;

public class AvroRecordValidatorTest {

    @Test
    public void testGoodRecord() throws Exception {
        String avsc = TestUtil.load("allavro/RecordWithNumericFields.avsc");
        Schema schema = AvroCompatibilityHelper.parse(avsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        GenericRecord record = (GenericRecord) generator.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));

        List<AvroRecordValidationIssue> issues = AvroRecordValidator.validate(record);
        Assert.assertTrue(issues.isEmpty());
    }

    @Test
    public void testBadPrimitiveValues() throws Exception {
        String avsc = TestUtil.load("allavro/RecordForValidation.avsc");
        Schema schema = AvroCompatibilityHelper.parse(avsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        GenericRecord record = (GenericRecord) generator.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));

        List<AvroRecordValidationIssue> issues = AvroRecordValidator.validate(record);
        Assert.assertTrue(issues.isEmpty());

        record.put("nullField", 7);
        issues = AvroRecordValidator.validate(record);
        Assert.assertEquals(issues.size(), 1);

        record.put("booleanField", null);
        record.put("optionalBooleanField", null);
        issues = AvroRecordValidator.validate(record);
        Assert.assertEquals(issues.size(), 2);

        //"reset"
        record = (GenericRecord) generator.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));

        record.put("intField", 7.0);
        record.put("stringField", Boolean.FALSE);
        issues = AvroRecordValidator.validate(record);
        Assert.assertEquals(issues.size(), 2);
    }

    @Test
    public void testMixedStrings() throws Exception {
        String avsc = TestUtil.load("allavro/RecordForValidation.avsc");
        Schema schema = AvroCompatibilityHelper.parse(avsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        GenericRecord record = (GenericRecord) generator.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));

        record.put("stringField", "im a string");
        record.put("optionalStringField", new Utf8("im also a string?"));

        List<AvroRecordValidationIssue> issues = AvroRecordValidator.validate(record);
        Assert.assertEquals(issues.size(), 1);
    }

    @Test
    public void testMixedGenericsAndSpecifics() throws Exception {
        GenericRecord outer = new GenericData.Record(RecordWithSelfReferences.SCHEMA$);
        RecordWithSelfReferences inner = new RecordWithSelfReferences();
        outer.put("selfRef", inner);

        List<AvroRecordValidationIssue> issues = AvroRecordValidator.validate(outer);
        Assert.assertEquals(issues.size(), 1);
    }
}
