/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.builder.operations.SchemaSet;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of SchemaSet that get schemas from ResolverPath
 */
public class ResolverPathSchemaSet implements SchemaSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResolverPathSchemaSet.class);

  private static final String AVRO_SUFFIX = "avsc";
  private static final String JAR_SUFFIX = "jar";
  private static final String ZIP_SUFFIX = "zip";

  private final Map<String, Schema> _allSchemas = new HashMap<>();

  public ResolverPathSchemaSet(Collection<File> resolverPath) throws IOException {
    for (File file : resolverPath) {
      loadSchemas(file, _allSchemas);
    }
  }

  @Override
  public int size() {
    return _allSchemas.size();
  }

  @Override
  public Schema getByName(String name) {
    return _allSchemas.get(name);
  }

  @Override
  public List<Schema> getAll() {
    return new ArrayList<>(_allSchemas.values());
  }

  @Override
  public void add(Schema schema) {
    _allSchemas.put(schema.getFullName(), schema);
  }

  /**
   * load avro schemas from File (avsc file, directory, Jar file, or Zip file)
   * @param file file object
   * @param schemas a map to hold all loaded schemas
   * @throws IOException when fail to read to the file
   */
  private static void loadSchemas(final File file, final Map<String, Schema> schemas) throws IOException {
    if (file.isDirectory()) {
      for (File child : Objects.requireNonNull(file.listFiles())) {
        loadSchemas(child, schemas);
      }
    } else if (file.getAbsolutePath().endsWith(AVRO_SUFFIX)) {
      Schema schema = parse(FileUtils.openInputStream(file));
      schemas.put(schema.getFullName(), schema);
    } else if (file.getAbsolutePath().endsWith(JAR_SUFFIX)) {
      loadSchemasFromCompressFile(file, schemas, true);
    } else if (file.getAbsolutePath().endsWith(ZIP_SUFFIX)) {
      loadSchemasFromCompressFile(file, schemas, false);
    } else {
      LOGGER.warn("Unrecognized dependency {}, ", file.getAbsolutePath());
    }
  }

  /**
   * load avro schemas from Jar or Zip file
   * @param file jar or zip file
   * @param schemas a map to hold all loaded schemas
   * @param isJar indicate the file is Jar or Zip
   * @throws IOException when fail to read to the file
   */
  private static void loadSchemasFromCompressFile(final File file, final Map<String, Schema> schemas, boolean isJar)
      throws IOException {
    try (ZipFile zipFile = isJar ? new JarFile(file) : new ZipFile(file)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (entry.getName().endsWith(AVRO_SUFFIX)) {
          Schema schema = parse(zipFile.getInputStream(entry));
          schemas.put(schema.getFullName(), schema);
        }
      }
    }
  }

  /**
   * parse avro schema from inputStream
   * @param inputStream inputStream that contains an avro schema
   * @return an avro schema
   * @throws IOException when fail to read to the stream
   */
  private static Schema parse(InputStream inputStream) throws IOException {
    return AvroCompatibilityHelper.parse(inputStream, SchemaParseConfiguration.LOOSE, null).getMainSchema();
  }
}