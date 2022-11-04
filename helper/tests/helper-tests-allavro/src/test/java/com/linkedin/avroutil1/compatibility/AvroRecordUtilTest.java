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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;
import under14.newnewpkg.inner.NewNewInnerRecordWithAliases;
import under14.newnewpkg.outer.NewNewOuterRecordWithAliases;
import under14.newpkg.outer.NewOuterRecordWithAliases;
import under14.oldpkg.inner.OldInnerRecordWithoutAliases;
import under14.oldpkg.outer.OldOuterRecordWithoutAliases;


public class AvroRecordUtilTest {

  @Test
  public void testSupplementDefaultsIntoGenericRecord() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    Schema schema = Schema.parse(avsc);

    GenericData.Record record = new GenericData.Record(schema);

    //there are unpopulated fields with no defaults, see that full population is thus impossible
    Assert.assertThrows(IllegalArgumentException.class, () -> AvroRecordUtil.supplementDefaults(record, true));

    //populate only those missing fields that have defaults
    AvroRecordUtil.supplementDefaults(record, false);

    //still missing values
    Assert.assertThrows(() -> AvroCodecUtil.serializeBinary(record));

    //provide values for (ONLY) those fields that dont have defaults in the schema
    record.put("boolWithoutDefault", true); //this starts out null
    record.put("strWithoutDefault", "I liek milk");

    //should now pass
    AvroCodecUtil.serializeBinary(record);
  }

  @Test
  public void testSupplementDefaultIntoSpecificRecord() throws Exception {
    under14.RecordWithDefaults record = new under14.RecordWithDefaults();

    //there are unpopulated fields with no defaults, see that full population is thus impossible
    Assert.assertThrows(IllegalArgumentException.class, () -> AvroRecordUtil.supplementDefaults(record, true));

    //populate only those missing fields that have defaults
    AvroRecordUtil.supplementDefaults(record, false);

    //still missing values
    Assert.assertThrows(() -> AvroCodecUtil.serializeBinary(record));

    //provide values for (ONLY) those fields that dont have defaults in the schema
    record.strWithoutDefault = "I liek milk";

    //should now pass
    AvroCodecUtil.serializeBinary(record);
  }

  @Test
  public void testTrivialGenericToSpecificConversion() throws Exception {
    Schema schema = under111.SimpleRecord.SCHEMA$;
    RandomRecordGenerator gen = new RandomRecordGenerator();
    GenericRecord genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    convertRoundTrip(genericInstance);
  }

  @Test
  public void testGenericToSpecific() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    Schema schema;
    GenericRecord genericInstance;

    schema = under14.RecordWithDefaults.SCHEMA$;
    genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(false));
    convertRoundTrip(genericInstance);

    schema = under14.HasComplexDefaults.SCHEMA$;
    genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    convertRoundTrip(genericInstance);
  }

  @Test
  public void testGenericToSpecificComplexCollections() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    Schema schema = under14.RecordWithCollectionsOfUnions.SCHEMA$;
    GenericRecord genericInstance = (GenericRecord) gen.randomGeneric(schema, genConfig);
    convertRoundTrip(genericInstance);
  }

  @Test
  public void testConversionsWithAliases() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();

    //specific --> generic

    OldOuterRecordWithoutAliases oldSpecific = gen.randomSpecific(OldOuterRecordWithoutAliases.class, RecordGenerationConfig.NO_NULLS);
    GenericRecord newGeneric = new GenericData.Record(NewOuterRecordWithAliases.getClassSchema());
    newGeneric = AvroRecordUtil.specificRecordToGenericRecord(oldSpecific, newGeneric, RecordConversionConfig.ALLOW_ALL_USE_UTF8);
    Assert.assertNotNull(newGeneric);
    Assert.assertNotEquals(newGeneric.getSchema(), oldSpecific.getSchema());

    GenericRecord newGenericInner = (GenericRecord) newGeneric.get("newOuterField");
    Assert.assertNotNull(newGenericInner);
    Assert.assertNotEquals(newGenericInner.getSchema(), OldInnerRecordWithoutAliases.getClassSchema());

    Assert.assertEquals(newGenericInner.get("newF1"), oldSpecific.outerField.f1);
    Assert.assertEquals(newGenericInner.get("newF2"), oldSpecific.outerField.f2);

    //generic --> specific

    GenericRecord oldGeneric = (GenericRecord) gen.randomGeneric(OldOuterRecordWithoutAliases.getClassSchema());
    SpecificRecord newSpecific = new NewOuterRecordWithAliases();
    newSpecific = AvroRecordUtil.genericRecordToSpecificRecord(oldGeneric, newSpecific, RecordConversionConfig.ALLOW_ALL_USE_UTF8);

    Assert.assertNotNull(newSpecific);
    Assert.assertNotEquals(newSpecific.getSchema(), oldGeneric.getSchema());

    SpecificRecord newSpecificInner = (SpecificRecord) newSpecific.get(newSpecific.getSchema().getField("newOuterField").pos());
    Assert.assertNotNull(newSpecificInner);
    Assert.assertNotEquals(newSpecificInner.getSchema(), OldInnerRecordWithoutAliases.getClassSchema());

    GenericRecord oldGenericInnder = (GenericRecord) oldGeneric.get("outerField");
    Assert.assertEquals(
        newSpecificInner.get(newSpecificInner.getSchema().getField("newF1").pos()),
        oldGenericInnder.get("f1")
    );
    Assert.assertEquals(
        newSpecificInner.get(newSpecificInner.getSchema().getField("newF2").pos()),
        oldGenericInnder.get("f2")
    );
  }

  @Test
  public void testConversionsWithAliasesAndUnions() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    OldOuterRecordWithoutAliases old = gen.randomSpecific(OldOuterRecordWithoutAliases.class, RecordGenerationConfig.NO_NULLS);
    GenericData.Record newRecord = new GenericData.Record(NewNewOuterRecordWithAliases.getClassSchema());
    GenericRecord genericRecord =
        AvroRecordUtil.specificRecordToGenericRecord(old, newRecord, RecordConversionConfig.ALLOW_ALL_USE_UTF8);
    Assert.assertNotNull(genericRecord);
    Assert.assertNotEquals(genericRecord.getSchema(), old.getSchema());

    GenericData.Record newInner = (GenericData.Record) genericRecord.get("newNewOuterField");
    Assert.assertNotNull(newInner);
    Assert.assertNotEquals(newInner.getSchema(), OldInnerRecordWithoutAliases.getClassSchema());

    Assert.assertEquals(Long.valueOf(String.valueOf(newInner.get("newNewF1"))), Long.valueOf(old.outerField.f1));
    Assert.assertEquals(newInner.get("newNewF2"), old.outerField.f2);

    //other direction (generic --> specific) tested in testStringTypeConversion() below
  }

  @Test
  public void testStringTypeConversion() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    GenericRecord oldGeneric = (GenericRecord) gen.randomGeneric(OldOuterRecordWithoutAliases.getClassSchema(), RecordGenerationConfig.NO_NULLS);
    NewNewOuterRecordWithAliases newSpecific = new NewNewOuterRecordWithAliases();
    newSpecific = AvroRecordUtil.genericRecordToSpecificRecord(oldGeneric, newSpecific, RecordConversionConfig.ALLOW_ALL_USE_STRING);
    Assert.assertNotNull(newSpecific);
    Assert.assertNotEquals(newSpecific.getSchema(), oldGeneric.getSchema());

    NewNewInnerRecordWithAliases newInner = newSpecific.newNewOuterField;
    Assert.assertNotNull(newInner);
    Assert.assertNotEquals(newInner.getSchema(), OldInnerRecordWithoutAliases.getClassSchema());

    GenericRecord oldGenericInner = (GenericRecord) oldGeneric.get("outerField");
    //int --> long widening
    Assert.assertEquals(newInner.newNewF1.longValue(), ((Integer) oldGenericInner.get("f1")).longValue());
    //Utf8 --> String conversion
    Assert.assertTrue(oldGenericInner.get("f2") instanceof Utf8);
    Assert.assertTrue(newInner.newNewF2 instanceof String);
    Assert.assertEquals(newInner.newNewF2, String.valueOf(oldGenericInner.get("f2")));
  }

  private void convertRoundTrip(GenericRecord original) {
    Assert.assertNotNull(original);
    SpecificRecord converted = AvroRecordUtil.genericRecordToSpecificRecord(original, null, RecordConversionConfig.ALLOW_ALL_USE_UTF8);
    Assert.assertNotNull(converted);
    GenericRecord backAgain = AvroRecordUtil.specificRecordToGenericRecord(converted, null, RecordConversionConfig.ALLOW_ALL_USE_UTF8);
    Assert.assertNotSame(original, backAgain);
    try {
      Assert.assertEquals(backAgain, original);
    } catch (AvroRuntimeException expected) {
      //avro 1.4 cant compare anything with map schemas for equality
      if (!expected.getMessage().contains("compare maps") || AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_4)) {
        Assert.fail("while attempting to compare generic records", expected);
      }
    }
  }
}
