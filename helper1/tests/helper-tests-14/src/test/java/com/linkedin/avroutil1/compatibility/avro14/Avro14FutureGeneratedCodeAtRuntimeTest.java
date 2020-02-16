package com.linkedin.avroutil1.compatibility.avro14;

import org.testng.annotations.Test;


public class Avro14FutureGeneratedCodeAtRuntimeTest {

  @Test(expectedExceptions = NoClassDefFoundError.class)
  public void demonstrateAvro19RecordUnusable() throws Exception {
    //fails to find class Schema$Parser
    new by19.SimpleRecord();
  }
}
