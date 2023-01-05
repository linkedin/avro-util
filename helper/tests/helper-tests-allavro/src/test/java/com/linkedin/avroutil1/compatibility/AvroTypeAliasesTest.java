/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroTypeAliasesTest {

  @Test
  public void demoAliasesToNullNamespace() throws Exception {
    AvroVersion avroVersion = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    boolean is1111 = avroVersion.equals(AvroVersion.AVRO_1_11);

    String avscInNull = TestUtil.load("allavro/aliasesToNullNamespace/RecordInNullNamespace.avsc");
    String avscWithAliases = TestUtil.load("allavro/aliasesToNullNamespace/RecordWithAliasesToNullNamespace.avsc");
    String avscWithDotlessAliases = TestUtil.load("allavro/aliasesToNullNamespace/RecordWithDotlessAliases.avsc");
    String avscWithoutAliases = TestUtil.load("allavro/aliasesToNullNamespace/RecordWithoutAliasesToNullNamespace.avsc");

    Schema schemaInNull = AvroCompatibilityHelper.parse(avscInNull, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema schemaWithAliases = AvroCompatibilityHelper.parse(avscWithAliases, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema schemaWithDotlessAliases = AvroCompatibilityHelper.parse(avscWithDotlessAliases, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Schema schemaWithoutAliases = AvroCompatibilityHelper.parse(avscWithoutAliases, SchemaParseConfiguration.STRICT, null).getMainSchema();

    //all have same simple name
    Assert.assertEquals(schemaInNull.getName(), schemaWithAliases.getName());
    Assert.assertEquals(schemaWithAliases.getName(), schemaWithoutAliases.getName());

    //assert namespace differences
    Assert.assertNull(schemaInNull.getNamespace());
    Assert.assertNotNull(schemaWithAliases.getNamespace());
    Assert.assertEquals(schemaWithAliases.getNamespace(), schemaWithoutAliases.getNamespace());

    //assert fullname differences
    Assert.assertNotEquals(schemaInNull.getFullName(), schemaWithAliases.getFullName());
    Assert.assertNotEquals(
        schemaInNull.getField("enumField").schema().getFullName(),
        schemaWithAliases.getField("enumField").schema().getFullName()
    );
    Assert.assertNotEquals(schemaInNull.getFullName(), schemaWithoutAliases.getFullName());
    Assert.assertNotEquals(
        schemaInNull.getField("enumField").schema().getFullName(),
        schemaWithoutAliases.getField("enumField").schema().getFullName()
    );

    //assert aliases
    assertNoAliases(schemaInNull);
    assertNoAliases(schemaInNull.getField("enumField").schema());
    assertNoAliases(schemaWithoutAliases);
    assertNoAliases(schemaWithoutAliases.getField("enumField").schema());

    if (avroVersion.earlierThan(AvroVersion.AVRO_1_7)) {
      //old avro preserves the dots
      Assert.assertEquals(schemaWithAliases.getAliases(), new HashSet<>(Collections.singletonList(".RecordWithNoNamespace")));
      Assert.assertEquals(schemaWithAliases.getField("enumField").schema().getAliases(), new HashSet<>(Collections.singletonList(".InnerEnum")));
    }
    //behaviour changes somewhere in 1.7.*, and we dont report minor versions, so cant assert anything
    if (avroVersion.laterThan(AvroVersion.AVRO_1_7)) {
      //modern avro strips the dots
      Assert.assertEquals(schemaWithAliases.getAliases(), new HashSet<>(Collections.singletonList("RecordWithNoNamespace")));
      Assert.assertEquals(schemaWithAliases.getField("enumField").schema().getAliases(), new HashSet<>(Collections.singletonList("InnerEnum")));
    }

    //schema with "dot-less" aliases means they are interpreted as "simple names" and the type's namespace applies
    //making the aliases worthless
    Set<String> topLevelDotlessAliases = schemaWithDotlessAliases.getAliases();
    Set<String> innerTypeDotlessAliases = schemaWithDotlessAliases.getField("enumField").schema().getAliases();
    Assert.assertEquals(topLevelDotlessAliases, new HashSet<>(Collections.singletonList(schemaWithAliases.getFullName())));
    Assert.assertEquals(innerTypeDotlessAliases, new HashSet<>(Collections.singletonList(schemaWithAliases.getField("enumField").schema().getFullName())));

    //re-generate avscWithAliases by calling Schema.toString(), compare the aliases on the 2nd copy vs the original
    Set<String> topLevelAliases = schemaWithAliases.getAliases();
    Set<String> innerTypeAliases = schemaWithAliases.getField("enumField").schema().getAliases();
    String avscWithAliases2 = schemaWithAliases.toString();
    Schema schemaWithAliases2 = AvroCompatibilityHelper.parse(avscWithAliases2, SchemaParseConfiguration.STRICT, null).getMainSchema();
    Set<String> topLevelAliases2 = schemaWithAliases2.getAliases();
    Set<String> innerTypeAliases2 = schemaWithAliases2.getField("enumField").schema().getAliases();
    //once again this changes in some 1.7.*
    if (avroVersion.laterThan(AvroVersion.AVRO_1_7)) {
      //modern avro mangles the aliases after a re-parse
      Assert.assertNotEquals(topLevelAliases2, topLevelAliases);
      Assert.assertNotEquals(innerTypeAliases2, innerTypeAliases);
    } else if (avroVersion.earlierThan(AvroVersion.AVRO_1_7)) {
      Assert.assertEquals(topLevelAliases2, topLevelAliases);
      Assert.assertEquals(innerTypeAliases2, innerTypeAliases);
    }

    //generate random record using the null namespace schema
    RandomRecordGenerator gen = new RandomRecordGenerator();
    GenericRecord original = (GenericRecord) gen.randomGeneric(schemaInNull);
    Assert.assertEquals(original.getSchema(), schemaInNull);

    //show aliases can be used to decode data, EXCEPT under avro 1.5/6 and early 1.7.* (1.7.7 works)
    if (avroVersion.laterThan(AvroVersion.AVRO_1_7)) {
      GenericRecord deserializedUsingAliases = serializationLoop(original, schemaWithAliases);
      Assert.assertEquals(deserializedUsingAliases.getSchema(), schemaWithAliases);

      //show the re-parsed schemas are bad because the aliases got mangled
      try {
        serializationLoop(original, schemaWithAliases2);
        //the above works under avro 1.11.1 ?! TODO - file bug
        if (!is1111) {
          Assert.fail("mangled schemas worked under " + avroVersion + "?!");
        }
      } catch (Exception expected) {
        //expected
      }
    }

    //show aliases are REQUIRED to decode data (by trying without)
    try {
      GenericRecord deserializedWithoutAliases = serializationLoop(original, schemaWithoutAliases);
      if (avroVersion.laterThan(AvroVersion.AVRO_1_4) && !is1111) {
        Assert.fail("decoding without aliases succeeded (?!) under " + avroVersion);
      } else {
        //avro 1.4 doesnt care about namespaces
        Assert.assertEquals(deserializedWithoutAliases.getSchema(), schemaWithoutAliases);
      }
    } catch (Exception expected) {
      String message = expected.getMessage();
      //the error is about the NESTED record, whereas it should have been about the top level record schemas mismatching.
      //however, no version of avro actually compares the top-level fullnames of reader vs writer ?!
      Assert.assertTrue(message.contains("InnerEnum"));
      Assert.assertFalse(message.contains(schemaWithAliases.getName()));
    }
  }

  private GenericRecord serializationLoop(IndexedRecord record, Schema readerSchema) throws Exception {
    byte[] serialized = AvroCodecUtil.serializeBinary(record);
    GenericRecord deserialized = AvroCodecUtil.deserializeAsGeneric(serialized, record.getSchema(), readerSchema);
    Assert.assertEquals(deserialized.getSchema(), readerSchema);
    return deserialized;
  }

  private void assertNoAliases(Schema schema) throws Exception {
    Assert.assertTrue(schema.getAliases() == null || schema.getAliases().isEmpty());
  }
}
