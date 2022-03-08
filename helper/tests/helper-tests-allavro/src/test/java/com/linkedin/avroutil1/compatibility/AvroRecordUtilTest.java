/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;
import under14.RecordWithDefaults;


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
    under14.RecordWithDefaults record = new RecordWithDefaults();

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
}
