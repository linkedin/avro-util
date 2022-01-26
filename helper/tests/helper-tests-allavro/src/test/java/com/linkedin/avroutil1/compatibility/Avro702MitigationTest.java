/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.Test;
import under14.middle.Avro702MiddleRecord;
import under14.outer.Avro702BadRecordWithAliases;
import under14.outer.Avro702CorrectedRecordWithAliases;
import under14.outer.Avro702InnerRecord;
import under14.outer.Avro702OuterRecordWithUnion;


/**
 * demonstrates avro-702 related issues and tests mitigation code
 */
public class Avro702MitigationTest {

    @Test
    public void demoIssueWithMitigatedBadSpecificRecordUnderVanillaAvro() throws Exception {
        //shows that data written using good schema CAN NOT be read by "bad" schema + aliases out of the box.
        //reason is that internally avro will try a Class.forName(readerSchemaFQCN) to find the specific record
        //classes to deserialize into. it will not find a reader class for the inner records since they generate
        //under their CORRECT fqcn (yes, even under 702), but the outer schema refers to them by a bad FQCN,
        //avro (in SpecificData.getClass(Schema schema)) ignores aliases for this lookup, when then
        //"punts to generic" and creates a GenericData$Record (wth?!) which causes a class cast later

        String goodAvsc = TestUtil.load("avro702/under14/Avro702BadRecordWithAliases-good.avsc");
        Schema goodSchema = Schema.parse(goodAvsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        IndexedRecord originalRecord = (IndexedRecord) generator.randomGeneric(
                goodSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        byte[] serialized = AvroCodecUtil.serializeBinary(originalRecord);
        try {
            AvroCodecUtil.deserializeAsSpecific(
                    serialized,
                    originalRecord.getSchema(),
                    Avro702BadRecordWithAliases.class
            );
            Assert.fail("expected to fail. avro is " + AvroCompatibilityHelper.getRuntimeAvroVersion());
        } catch (ClassCastException expected) {
            Assert.assertTrue(
                    expected.getMessage().contains("org.apache.avro.generic.GenericData$Record cannot be cast"),
                    "unexpected exception (" + expected.getMessage() + ")"
            );
        }
    }

    @Test
    public void testInteroperabilityOfMitigatedBadSpecificRecord() throws Exception {
        //shows data written with good schema can be read by "bad" schema + aliases
        String goodAvsc = TestUtil.load("avro702/under14/Avro702BadRecordWithAliases-good.avsc");
        Schema goodSchema = Schema.parse(goodAvsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        IndexedRecord originalRecord = (IndexedRecord) generator.randomGeneric(
                goodSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        byte[] serialized = AvroCodecUtil.serializeBinary(originalRecord);
        Avro702BadRecordWithAliases deserialized = AvroCodecUtil.deserializeAsSpecificWithAliases(
                serialized,
                originalRecord.getSchema(),
                Avro702BadRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNotNull(deserialized.outerField.middleEnumField);

        //repeat with an "older" good record (fewer fields)
        String goodOlderAvsc = TestUtil.load("avro702/under14/Avro702BadRecordWithAliases-older-good.avsc");
        Schema goodOlderSchema = Schema.parse(goodOlderAvsc);
        originalRecord = (IndexedRecord) generator.randomGeneric(
                goodOlderSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        serialized = AvroCodecUtil.serializeBinary(originalRecord);
        deserialized = AvroCodecUtil.deserializeAsSpecificWithAliases(
                serialized,
                originalRecord.getSchema(),
                Avro702BadRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNull(deserialized.outerField.middleEnumField); //default value used for added field in reader schema

        //repeat with a "newer" good record (more fields)
        String goodNewerAvsc = TestUtil.load("avro702/under14/Avro702BadRecordWithAliases-newer-good.avsc");
        Schema goodNewerSchema = Schema.parse(goodNewerAvsc);
        originalRecord = (IndexedRecord) generator.randomGeneric(
                goodNewerSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        serialized = AvroCodecUtil.serializeBinary(originalRecord);
        deserialized = AvroCodecUtil.deserializeAsSpecificWithAliases(
                serialized,
                originalRecord.getSchema(),
                Avro702BadRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNotNull(deserialized.outerField.middleEnumField);
    }

    @Test
    public void testInteroperabilityOfMitigatedCorrectedSpecificRecord() throws Exception {
        //shows data written with "bad" schema can be read by correct schema + aliases
        String badAvsc = TestUtil.load("avro702/under14/Avro702CorrectedRecordWithAliases-bad.avsc");
        Schema badSchema = Schema.parse(badAvsc);
        RandomRecordGenerator generator = new RandomRecordGenerator();
        IndexedRecord originalRecord = (IndexedRecord) generator.randomGeneric(
                badSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        byte[] serialized = AvroCodecUtil.serializeBinary(originalRecord);
        Avro702CorrectedRecordWithAliases deserialized = AvroCodecUtil.deserializeAsSpecific(
                serialized,
                originalRecord.getSchema(),
                Avro702CorrectedRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNotNull(deserialized.outerField.middleEnumField);

        //repeat with an "older" bad record (fewer fields)
        String badOlderAvsc = TestUtil.load("avro702/under14/Avro702CorrectedRecordWithAliases-older-bad.avsc");
        Schema badOlderSchema = Schema.parse(badOlderAvsc);
        originalRecord = (IndexedRecord) generator.randomGeneric(
                badOlderSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        serialized = AvroCodecUtil.serializeBinary(originalRecord);
        deserialized = AvroCodecUtil.deserializeAsSpecific(
                serialized,
                originalRecord.getSchema(),
                Avro702CorrectedRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNull(deserialized.outerField.middleEnumField); //default value used for added field in reader schema

        //repeat with a "newer" bad record (more fields)
        String badNewerAvsc = TestUtil.load("avro702/under14/Avro702CorrectedRecordWithAliases-newer-bad.avsc");
        Schema badNewerSchema = Schema.parse(badNewerAvsc);
        originalRecord = (IndexedRecord) generator.randomGeneric(
                badNewerSchema,
                RecordGenerationConfig.newConfig().withAvoidNulls(true)
        );
        serialized = AvroCodecUtil.serializeBinary(originalRecord);
        deserialized = AvroCodecUtil.deserializeAsSpecific(
                serialized,
                originalRecord.getSchema(),
                Avro702CorrectedRecordWithAliases.class
        );
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.outerField.middleRecordField);
        Assert.assertNotNull(deserialized.outerField.middleEnumField);
    }

    @Test
    public void showSerializationFailureWithUnmitigatedSpecificRecords() throws Exception {
        Avro702OuterRecordWithUnion outerRecord = new Avro702OuterRecordWithUnion();
        Avro702MiddleRecord middleRecord = new Avro702MiddleRecord();
        Avro702InnerRecord innerRecord = new Avro702InnerRecord();
        innerRecord.innerField = 42;
        middleRecord.middleField = innerRecord;
        outerRecord.outerField = middleRecord;

        showSerializationFailure(outerRecord);
    }

    @Test
    public void showSerializationFailureWithUnmitigatedGenericRecords() throws Exception {
        GenericData.Record outerRecord = new GenericData.Record(Avro702OuterRecordWithUnion.getClassSchema());
        GenericData.Record middleRecord = new GenericData.Record(Avro702MiddleRecord.getClassSchema());
        GenericData.Record innerRecord = new GenericData.Record(Avro702InnerRecord.getClassSchema());
        innerRecord.put("innerField", 42);
        middleRecord.put("middleField", innerRecord);
        outerRecord.put("outerField",  middleRecord);

        showSerializationFailure(outerRecord);
    }

    private void showSerializationFailure(IndexedRecord outerRecord) throws Exception {
        Schema.Field outerField = outerRecord.getSchema().getField("outerField");
        Schema.Field middleField = outerField.schema().getField("middleField");
        Schema embeddedInnerSchema = middleField.schema().getTypes().get(1);

        Assert.assertEquals(embeddedInnerSchema.getNamespace(), "under14.middle"); //wrong namespace

        IndexedRecord innerRecord = (IndexedRecord) ((IndexedRecord) outerRecord.get(outerField.pos())).get(middleField.pos());
        Schema innerSchema = innerRecord.getSchema();

        Assert.assertEquals(innerSchema.getNamespace(), "under14.outer"); //correct namespace

        AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
        try {
            //ancient avro resolves unions by simple name, so doesnt care about namespaces.
            //modern avro fails since the schema of innerRecord does not match the expected
            //one by fullname
            AvroCodecUtil.serializeBinary(outerRecord);
            Assert.assertEquals(runtimeAvroVersion, AvroVersion.AVRO_1_4, "expected to fail under " + runtimeAvroVersion);
        } catch (AvroRuntimeException expected) {
            //this exception class doesnt exist in 1.4, so reflection
            Assert.assertEquals(expected.getClass().getName(), "org.apache.avro.UnresolvedUnionException");
            Assert.assertTrue(expected.getMessage().contains("Not in union"));
            Assert.assertTrue(runtimeAvroVersion.laterThan(AvroVersion.AVRO_1_4),
                    "was not expecting an exception under " + runtimeAvroVersion);
        }
    }
}
