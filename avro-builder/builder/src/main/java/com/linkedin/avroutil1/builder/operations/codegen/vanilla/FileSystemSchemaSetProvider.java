/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.builder.operations.SchemaSet;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SchemaVisitor;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Loads schemas defined in a set of files.
 *
 * <p>
 * The files to load are described through two parameters: <i>dirs</i> and <i>common</i>.
 * Each is a list of files/directories which is expanded to a list of files.  Directories
 * are traversed such that all descendant files are included.  The files from <i>common</i>
 * are loaded first.  All schemas found within the files are loaded.  Next the files from
 * <i>dirs</i> is loaded.  All schemas found within these files are loaded too.
 * </p>
 *
 * <p>
 * For <i>common</i> types, references to types defined in other files is allowed.  The rules are pretty simple:
 * 1) No circular references (i.e. A references B and B references A).
 * 2) Types defined in <i>common</i> may reference other types defined in <i>common</i>.
 * 3) Types defined in <i>dirs</i> may reference types defined in <i>common</i> and in <i>dirs</i>.
 * </p>
 *
 * <p>
 * Some consequences of this:
 * 1) Types defined in <i>dirs</i> may reference other types defined in <i>dirs</i>.
 * 2) Types defined in <i>common</i> may NOT reference types defined in <i>dirs</i>.
 * </p>
 *
 */
public class FileSystemSchemaSetProvider implements SchemaSetProvider {

  public static final String DEFAULT_SCHEMA_SUFFIX = ".avsc";

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemSchemaSetProvider.class);
  //this is the avro error that indicates the definition of some record type was not found
  private static final Pattern UNDEFINED_TYPE_PATTERN = Pattern.compile("\"([\\w.]+)\" is not a defined name");
  //this is the avro error that indicates the definition of some record type was not found inside a union
  private static final Pattern UNDEFINED_NAME_PATTERN = Pattern.compile("Undefined name: \"([\\w.]+)\"");
  private static final Set<Schema.Type> AVRO_NAMED_TYPES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(Schema.Type.ENUM, Schema.Type.FIXED, Schema.Type.RECORD)));

  private final String schemaSuffix;
  private final List<File> nonImportableRoots;
  private final List<File> dirs;
  private final SchemaSet fallbackSchemaSet; //any schemas not found (defined) anywhere else will be looked-up here

  enum ParseErrorHandling {Fail, Retry}

  public FileSystemSchemaSetProvider(List<File> dirs) {
    this(dirs, new ArrayList<>(), DEFAULT_SCHEMA_SUFFIX);
  }

  public FileSystemSchemaSetProvider(List<File> dirs, List<File> common) {
    this(dirs, common, DEFAULT_SCHEMA_SUFFIX);
  }

  public FileSystemSchemaSetProvider(List<File> dirs, List<File> nonImportableRoots, String schemaSuffix) {
    this(dirs, nonImportableRoots, schemaSuffix, null);
  }

  public FileSystemSchemaSetProvider(List<File> dirs, List<File> nonImportableRoots, String schemaSuffix,
      SchemaSet fallbackSchemaSet) {
    this.dirs = dirs;
    this.nonImportableRoots = nonImportableRoots;
    this.schemaSuffix = schemaSuffix;
    this.fallbackSchemaSet = fallbackSchemaSet;
  }

  @Override
  public SchemaSet loadSchemas(boolean topLevelOnly) {
    try {
      // Retry on parse errors so that types in common may reference other types also defined in common.
      Collection<Schema> allCommonSchemas;
      SchemaSet schemas = new SimpleSchemaSet();
      loadSchemas(buildFileList(dirs), schemas, true, new ArrayList<>(), ParseErrorHandling.Retry);
      List<Schema> allRootCommonSchemas = schemas.getAll(); //dup-free
      allCommonSchemas = AvroSchemaUtil.getAllDefinedSchemas(allRootCommonSchemas).values(); //dup-free

      if (nonImportableRoots != null && !nonImportableRoots.isEmpty()) {
        // Retry on parse errors so that types in dirs may reference other types also defined in dirs.
        loadSchemas(buildFileList(nonImportableRoots), schemas, topLevelOnly, allCommonSchemas,
            ParseErrorHandling.Retry);
      }
      return schemas;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * loads schemas from a given set of input avsc files, given a set of known common schemas, into
   * the given SchemaSet
   * @param inputFiles input avsc files
   * @param schemas output schema set
   * @param topLevelOnly true to return only top level (per-file) schemas. false
   *                     to also load nested schemas individually
   * @param common known schemas (already defined elsewhere that we allow "importing" here)
   * @param option behavious on parse error
   * @throws IOException
   */
  void loadSchemas(List<File> inputFiles, SchemaSet schemas, boolean topLevelOnly, Collection<Schema> common,
      ParseErrorHandling option) throws IOException {

    //FQCNs of schemas that avro has not found during the pass
    Set<String> missingSchemaFQCNs = new HashSet<>();
    List<File> files = inputFiles;
    Set<String> identicalDupSchemaNames = new HashSet<>(0); //schemas that are copy-pasted around, but identical :-(
    while (files.size() > 0) {
      missingSchemaFQCNs.clear();
      List<Exception> parseExceptions = new ArrayList<Exception>();
      List<File> failedFiles = new ArrayList<File>();
      int successfullyParsed =
          0; //avsc files we have successfully parsed this pass (meaning we had all their dependencies)

      // 'successfullyParsedSchemas' starts out having just the contents of 'common'.
      // It grows with each iteration of the while loop.
      Map<String, Schema> successfullyParsedSchemas = new HashMap<>(common.size() + schemas.size());
      for (Schema commonSchema : common) {
        Schema other = successfullyParsedSchemas.put(commonSchema.getFullName(), commonSchema);
        if (other != null) {
          throw new IllegalArgumentException(
              "duplicate schema definition found for " + commonSchema.getFullName() + ": " + other + " vs "
                  + commonSchema);
        }
      }
      for (Schema previouslyParsed : schemas.getAll()) {
        String fqcn = previouslyParsed.getFullName();
        Schema other = successfullyParsedSchemas.put(fqcn, previouslyParsed);
        if (other != null && !(identicalDupSchemaNames.contains(fqcn))) {
          throw new IllegalArgumentException(
              "duplicate parsed schema definition found for " + fqcn + ": common " + other + " vs input "
                  + previouslyParsed);
        }
      }

      //make a pass of all "uncompiled" files trying to compile them. in theory at least one should succeed
      //(this code is not smart about order of dependencies, so O(n^2))

      for (File file : files) {
        try {
          loadSchema(file, schemas, topLevelOnly, successfullyParsedSchemas.values());
          //see if schemas now has anything thats a dup of known schemas
          for (Schema newlyParsed : schemas.getAll()) {
            String fqcn = newlyParsed.getFullName();
            Schema parsedBefore = successfullyParsedSchemas.get(fqcn);
            if (parsedBefore != null && !identicalDupSchemaNames.contains(fqcn)) {
              identicalDupSchemaNames.add(fqcn);
              if (!parsedBefore.equals(newlyParsed)) {
                throw new IllegalArgumentException(
                    "duplicate DIFFERENT schema definitions found while parsing " + file.getAbsolutePath() + " for "
                        + fqcn + ": " + newlyParsed + " vs " + parsedBefore);
              }
            }
          }
          successfullyParsed++;
        } catch (SchemaParseException e) {
          String message = e.getMessage();
          if (message != null && message.contains("Can't redefine:")) {
            //this exception indicates a duplicate definition. these are fatal
            throw new IllegalArgumentException(
                "duplicate schema definition found while parsing " + file.getAbsolutePath(), e);
          }
          if (option == ParseErrorHandling.Retry) {
            // This exception could indicate that a type is referenced in the schema which is not yet
            // in our list of common schemas (because we haven't loaded it yet). Add the file to
            // our list of failed files so we can try again.
            failedFiles.add(file);
            parseExceptions.add(e);
            recordMissingSchemaFQCNs(e, missingSchemaFQCNs);
          } else {
            throw e;
          }
        }
      }

      // did we make any progress this round?
      if (successfullyParsed == 0 && !findMissingSchemaOnCP(missingSchemaFQCNs, schemas)) {
        //did we still fail to make any progress ? throw the 1st exception encountered as the cause.
        Exception e = parseExceptions.get(0);
        File file = failedFiles.get(0);
        throw new RuntimeException(String.format("Failed to load file %s: %s", file, e.getMessage()),
            parseExceptions.get(0));
      }

      files = failedFiles;
    }
  }

  /**
   * attempts to load 1 (and only 1) schema from the fallback schemaset, if defined.
   * @param schemasMissing set of FQCNs of schemas not found
   * @param output schema set to add any found schema(s) to
   * @return true if anything found
   */
  private boolean findMissingSchemaOnCP(Set<String> schemasMissing, SchemaSet output) {
    if (fallbackSchemaSet == null) {
      return false;
    }
    for (String fqcn : schemasMissing) {
      Schema found = fallbackSchemaSet.getByName(fqcn);
      if (found != null) {
        //find (recursively) all schemas "rooted" (included) in this schema we just found
        //this is so we wont generate code for any transitives (for example, if we find
        //"EventHeader" on the classpath, we also want to avoid generating Guid.java)
        AvroSchemaUtil.traverseSchema(found, new SchemaVisitor() {
          @Override
          public void visitSchema(Schema schema) {
            Schema.Type type = schema.getType();
            if (!AVRO_NAMED_TYPES.contains(type)) {
              //we only care about named schemas we encounter.
              //anything else cannot be imported anyway
              return;
            }
            if (fallbackSchemaSet.getByName(schema.getFullName()) != null) {
              output.add(schema);
            }
          }
        });
        //we stop at the 1st one found. this is on purpose - although in theory if found overlaps
        //with the set of schemas being generated the user project has serious problems anyway (classes
        //defined more than once) we still try and prioritize "local definitions"
        return true;
      }
    }
    return false;
  }

  /**
   * given an avro parse exception, see if it indicates some missing schema, and record any missing schemas
   * @param e an avro parse exception
   * @param output set of missing schema FQCNs to add to
   */
  private void recordMissingSchemaFQCNs(SchemaParseException e, Set<String> output) {
    String msg = e.getMessage();
    Matcher matcher = UNDEFINED_TYPE_PATTERN.matcher(msg);
    if (matcher.find()) {
      String fqcn = matcher.group(1);
      output.add(fqcn);
      return;
    }
    matcher = UNDEFINED_NAME_PATTERN.matcher(msg);
    if (matcher.find()) {
      String fqcn = matcher.group(1);
      output.add(fqcn);
    }
  }

  /**
   * attempts to load/parse any schemas defined in the input avsc file, given a list of known schemas,
   * and stores any newly loaded schemas from the file into the given output SchemaSet
   * @param f input avsc file
   * @param schemas output schema set to receive any successfully parsed schemas out of the file
   * @param topLevelOnly true to load only top-level (per-file) schemas.
   *                     false to load all nested schemas individually as well
   * @param known set of previously successfully parsed schemas
   * @throws IOException on errors
   */
  private void loadSchema(File f, SchemaSet schemas, boolean topLevelOnly, Collection<Schema> known)
      throws IOException {
    if (!f.exists() || !f.canRead()) {
      throw new IllegalArgumentException("File does not exist or cannot be read: " + f.getAbsolutePath());
    }

    if (!f.getName().endsWith(schemaSuffix)) {
      throw new IllegalArgumentException("File does not end in " + schemaSuffix);
    }

    LOGGER.debug("Loading schemas from {}", f);

    try (InputStream input = new FileInputStream(f)) {
      String schemaJson = IOUtils.toString(input, StandardCharsets.UTF_8);
      SchemaParseResult result = AvroCompatibilityHelper.parse(schemaJson, SchemaParseConfiguration.STRICT, known);
      Schema parsedSchema = result.getMainSchema();
      //add successfully parsed schema (and maybe all nested schemas as well)
      if (topLevelOnly) {
        schemas.add(parsedSchema);
      } else {
        Collection<Schema> allContainedTherein = AvroSchemaUtil.getAllDefinedSchemas(parsedSchema).values();
        for (Schema contained : allContainedTherein) {
          schemas.add(contained);
        }
      }
    } catch (Exception e) {
      //this catches json processing exceptions (from jackson 1.* or 2.*, as different versions of avro use either)
      //this DOES NOT catch avro exceptions (that may happen after json parsing is done) as they are handled by the caller
      // We check the name by its String value not its class value in order to not directly depend on a specific version of Jackson.
      if (e.getClass().getName().equals("com.fasterxml.jackson.core.JsonProcessingException")) {
        LOGGER.error("exception parsing avro file {}", f.getAbsolutePath(), e);
        throw new IllegalArgumentException("exception parsing avro file " + f.getAbsolutePath(), e);
      } else {
        throw e;
      }
    }
  }

  /**
   * Build a list of files from another list of files/directories.  If a file is provided then it is added to
   * the returned list.  If a directory is provided then all files which are descendants of the directory are added to the list.
   *
   * @param input
   * @return
   */
  private List<File> buildFileList(List<File> input) {
    List<File> files = new ArrayList<>();
    for (File f : input) {
      addToFileList(f, files);
    }
    return files;
  }

  private void addToFileList(File f, List<File> current) {
    if (!f.exists() || !f.canRead()) {
      throw new IllegalArgumentException("File does not exist or cannot be read: " + f.getAbsolutePath());
    }

    if (f.isDirectory()) {
      File[] files = f.listFiles();
      if (files != null) {
        for (File child : files) {
          addToFileList(child, current);
        }
      }
    } else if (f.getName().endsWith(schemaSuffix)) {
      current.add(f);
    } else {
      LOGGER.debug("Ignoring file " + f + " as it does not end with the required suffix '" + schemaSuffix + "'.");
    }
  }
}
