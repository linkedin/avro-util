/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.acme.generatedby14.Fixed;
import com.linkedin.avro.TestUtil;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import java.io.PrintWriter;
import java.io.StringWriter;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.AvroRuntimeException;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


/**
 * this class (intended to run under modern avro) tests the compatibility (or lack thereof) of
 * code generated under avro 1.4 with modern avro
 */
public class Avro14FactoryCompatibilityTest {

  @Test
  public void testTransformedFixedClassesCompatibleWithModernAvro() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (!runtimeVersion.laterThan(AvroVersion.AVRO_1_4)) {
      throw new SkipException("class only supported under modern avro. runtime version detected as " + runtimeVersion);
    }

    Fixed instance = new Fixed(); //would explode if generated with vanilla 1.4
    Assert.assertNotNull(instance);
  }

  @Test
  public void testVanilla14FixedClassesIncompatibleWithAvro17() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (!runtimeVersion.equals(AvroVersion.AVRO_1_7)) {
      throw new SkipException("class only supported under avro 1.7. runtime version detected as " + runtimeVersion);
    }

    String sourceCode = TestUtil.load("Vanilla14Fixed");
    Class clazz = CompilerUtils.CACHED_COMPILER.loadFromJava("com.acme.generatedby14.Vanilla14Fixed", sourceCode);
    try {
      clazz.newInstance();
      Assert.fail("expecting an exception");
    } catch (AvroRuntimeException expected) {
      Assert.assertTrue(expected.getMessage().contains("Not a Specific class")); //fails to find SCHEMA$
    }
  }

  @Test
  public void testVanilla14FixedClassesIncompatibleWithModernAvro() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (!runtimeVersion.laterThan(AvroVersion.AVRO_1_7)) {
      throw new SkipException("class only supported under modern avro. runtime version detected as " + runtimeVersion);
    }

    String sourceCode = TestUtil.load("Vanilla14Fixed");
    StringWriter sr = new StringWriter();
    PrintWriter compilerOutput = new PrintWriter(sr);
    try {
      Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(getClass().getClassLoader(),
          "com.acme.generatedby14.Vanilla14Fixed", sourceCode, compilerOutput);
      Assert.fail("compilation expected to fail");
    } catch (ClassNotFoundException ignored) {
      //expected
    }
    String errorMsg = sr.toString();
    Assert.assertTrue(errorMsg.contains("is not abstract and does not override")); //doesnt implement Externalizable
  }
}
