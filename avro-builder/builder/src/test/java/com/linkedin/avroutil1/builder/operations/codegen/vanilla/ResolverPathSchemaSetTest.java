/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class is to test {@link ResolverPathSchemaSet}
 */
public class ResolverPathSchemaSetTest {

  @Test
  public void testCreateSchemaSetAndLookup() throws Exception {
    // resolverPath1 is an avsc file
    File resolverPath1 = getFile("test-schemaset/SimpleRecord.avsc");

    // resolverPath2 is a directory that contains one avsc file
    File resolverPath2 = getFile("test-schemaset/schema/");

    // resolverPath3 is a Jar that contains two avsc files (SimpleEnum.avsc and SimpleFixed.avsc)
    File resolverPath3 = getFile("test-schemaset/avro_jar.jar");

    // resolverPath4 is a Zip that contains two avsc files (RecordWithSimpleDefault.avsc and RecordWIthUnion.avsc)
    File resolverPath4 = getFile("test-schemaset/avro_zip.zip");

    ResolverPathSchemaSet resolverPathSchemaSet =
        new ResolverPathSchemaSet(Arrays.asList(resolverPath1, resolverPath2, resolverPath3, resolverPath4));

    // look up each schema added to the resolver path
    for (String fileName : Arrays.asList("test-schemaset/SimpleRecord.avsc", "test-schemaset/schema/ExampleRecord.avsc",
        "test-schemaset/jar/avro/SimpleEnum.avsc", "test-schemaset/jar/SimpleFixed.avsc",
        "test-schemaset/zip/avro/RecordWithSimpleDefaults.avsc", "test-schemaset/zip/RecordWithUnion.avsc")) {

      Schema schema = loadSchema(fileName);

      Schema lookupSchema = resolverPathSchemaSet.getByName(schema.getFullName());

      Assert.assertEquals(lookupSchema, schema);
    }

    // look up a schema that is not included in the resolver path should fail
    Assert.assertNull(resolverPathSchemaSet.getByName("UnKnownSchema"));
  }

  /**
   * load a schema from test resource
   * @param fileName name of the avsc file in the test resource
   * @return a Schema object
   * @throws IOException when fail to read to the file
   */
  private static Schema loadSchema(final String fileName) throws IOException {
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
      return AvroCompatibilityHelper.parse(inputStream, SchemaParseConfiguration.LOOSE, null).getMainSchema();
    }
  }

  /**
   * Get the File object for a test resource file or directory
   * @param pathName name of test resource file or directory
   * @return the File object for a test resource file or directory
   */
  private static File getFile(final String pathName) {
    return new File(
        Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(pathName)).getFile());
  }
}