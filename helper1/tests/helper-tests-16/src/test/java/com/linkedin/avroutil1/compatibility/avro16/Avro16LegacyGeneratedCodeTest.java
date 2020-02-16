package com.linkedin.avroutil1.compatibility.avro16;

import com.google.common.base.Throwables;
import org.apache.avro.AvroRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro16LegacyGeneratedCodeTest {

  @Test
  public void demonstrateAvro14FixedUnusableUnder16() throws Exception {
    //avro fixed classes extend org.apache.avro.specific.SpecificFixed which, in turn implements
    //org.apache.avro.generic.GenericFixed. in avro 1.5+ GenericFixed extends org.apache.avro.generic.GenericContainer.
    //GenericContainer, in turn, defined method getSchema() that avro-14-generated fixed classes dont implement
    //under 1.6 specifically the failure is a little different - its looking for field SCHEMA$ directly 1st.
    //avro swallows the real root cause (NoSuchFieldException) though - #craftsmanship
    try {
      new by14.SimpleFixed();
      Assert.fail("expected to throw");
    } catch (AvroRuntimeException issue) {
      Throwable root = Throwables.getRootCause(issue);
      Assert.assertTrue(root instanceof AvroRuntimeException);
      Assert.assertTrue(root.getMessage().contains("Not a Specific class"));
    }
  }
}
