package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import javax.lang.model.SourceVersion;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;


public class Utils {
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER = new ArrayList<>();
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER = new ArrayList<>();

  private static final String REPLACEMENT_SEPARATOR = Matcher.quoteReplacement(File.separator);

  private static final boolean IS_WINDOWS = System.getProperty("os.name")
          .toLowerCase(Locale.ROOT)
          .contains("windows");

  static {
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_4);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_5);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_6);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_8);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_9);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_10);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_11);

    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.addAll(AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER);
  }

  // Cache the mapping between Schema and the corresponding fingerprint
  private static final Map<Schema, Integer> SCHEMA_IDS_CACHE = new ConcurrentHashMap<>();

  // Limit max schema length in WARNING logs.
  static final int MAX_SCHEMA_LENGTH_IN_WARNING = 100;

  private Utils() {
  }

  public static boolean isAvro14() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_4);
  }

  // enum default was added in avro 1.9
  public static boolean isAbleToSupportEnumDefault() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_8);
  }

  public static boolean isAbleToSupportStringableProps() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_6);
  }

  public static boolean isAbleToSupportJavaStrings() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_5);
  }

  public static boolean isSupportedAvroVersionsForDeserializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());
  }

  public static boolean isSupportedAvroVersionsForSerializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());
  }

  public static boolean isLogicalTypeSupported() {
    // Formally Avro_1.8 supports LogicalTypes however there are significant changes compared to versions >=1.9
    // To see rationale simply compare imports in org.apache.avro.data.TimeConversions class between 1.8 and 1.9+
    // Basically 1.8 uses joda.time but 1.9+ uses java.time
    return AvroCompatibilityHelperCommon.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_8);
  }

  public static boolean usesQualifiedNameForNamedTypedMatching(){
    return AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_5)
            || AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_6)
            || AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_7);
  }

  public static boolean isWindows() {
    return IS_WINDOWS;
  }

  public static AvroVersion getRuntimeAvroVersion() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion();
  }

  public static List<AvroVersion> getAvroVersionsSupportedForDeserializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER;
  }

  public static List<AvroVersion> getAvroVersionsSupportedForSerializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER;
  }

  public static String generateSourcePathFromPackageName(String packageName) {
    StringBuilder pathBuilder = new StringBuilder(File.separator);
    Arrays.stream(packageName.split("\\.")).forEach( s -> pathBuilder.append(s).append(File.separator));
    return pathBuilder.toString();
  }

  /**
   * given a filesystem path (assumed to use either forward or backward slashes, but not both)
   * this returns the same path, but using the separator character that matches the current
   * runtime OS.
   * @param path a path using some (single) separator
   * @return the input path, now using the separator matching the current operating system.
   */
  public static String fixSeparatorsToMatchOS(String path) {
    if (path == null || path.contains(File.separator)) {
      return path;
    }

    //noinspection RegExpRedundantEscape
    return path.replaceAll("[\\/]", REPLACEMENT_SEPARATOR);
  }
  /**
   * This function will produce a fingerprint for the provided schema.
   *
   * @param schema a schema
   * @return fingerprint for the given schema
   */
  public static int getSchemaFingerprint(Schema schema) {
    Integer schemaId = SCHEMA_IDS_CACHE.get(schema);
    if (schemaId == null) {
      String schemaString = AvroCompatibilityHelper.toAvsc(schema, AvscGenerationConfig.CORRECT_ONELINE);
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(schemaString.getBytes());
        int scratchPad = 0;
        for (int i = 0; i < digest.length; i++) {
          scratchPad = (scratchPad * 256 + (digest[i] & 0xFF));
        }
        schemaId = scratchPad;
      } catch (NoSuchAlgorithmException e) {
        schemaId = schemaString.hashCode();
      }

      SCHEMA_IDS_CACHE.put(schema, schemaId);
    }

    return schemaId;
  }

  private static String replaceLast(String str, char target, char replacement) {
    if (str.indexOf(target) < 0) {
      // doesn't contain target char
      return str;
    }
    int lastOccurrence = str.lastIndexOf(target);
    StringBuilder sb = new StringBuilder();
    sb.append(str, 0, lastOccurrence)
        .append(replacement);
    if (lastOccurrence != str.length() - 1) {
      sb.append(str.substring(lastOccurrence + 1));
    }
    return sb.toString();
  }

  private static Class<?> loadClass(String className) throws ClassNotFoundException {
    try {
      // First try the current class loader
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      // If the required class couldn't be found, here will try to use the class loader of current thread
      return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
    }
  }

  /**
   * This class is used to infer all the compilation dependencies.
   * @param existingCompileClasspath existing compile classpath
   * @param filePath java source file to compile
   * @param knownUsedFullyQualifiedClassNameSet: known fully qualified class name when generating the serialization/de-serialization classes
   * @return classpath to compile given file
   * @throws IOException on io issues
   * @throws ClassNotFoundException on classloading issues
   */
  public static String inferCompileDependencies(String existingCompileClasspath, String filePath, Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    Set<String> usedFullyQualifiedClassNameSet = new HashSet<>(knownUsedFullyQualifiedClassNameSet);
    Set<String> libSet = Arrays.stream(existingCompileClasspath.split(File.pathSeparator)).collect(Collectors.toSet());
    final String importPrefix = "import ";
    // collect all the necessary dependencies for compilation
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(importPrefix)) {
          // Get the qualified class name from "import" statements
          String qualifiedClassName = line.substring(importPrefix.length(), line.length() - 1);

          if (!qualifiedClassName.isEmpty()) {
            usedFullyQualifiedClassNameSet.add(qualifiedClassName);
          }
        }
      }

      StringJoiner pathJoiner = new StringJoiner(File.pathSeparator);
      if (existingCompileClasspath != null && !existingCompileClasspath.isEmpty()) {
        pathJoiner.add(existingCompileClasspath);
      }
      for (String requiredClass : usedFullyQualifiedClassNameSet) {
        CodeSource codeResource;
        try {
          codeResource = loadClass(requiredClass).getProtectionDomain().getCodeSource();
        } catch (ClassNotFoundException e) {
          /**
           * Inner class couldn't be located directly by Class.forName in the formal way, so we have to replace the
           * last '.' by '$'.
           * For example, 'org.apache.avro.generic.GenericData.Record' could NOT be located by {@link Class#forName(String)},
           * but 'org.apache.avro.generic.GenericData#Record' can be located.
           *
           * Here, we only try once since multiple layers of inner class is not expected in the generated java class,
           * and in theory, the inner class being used could only be defined by Avro lib.
           * If this assumption is not right, we need to do recursive search to find the right library.
           */
          codeResource = loadClass(replaceLast(requiredClass, '.', '$')).getProtectionDomain().getCodeSource();
        }
        if (codeResource != null) {
          String libPath = codeResource.getLocation().getFile();
          if (isWindows() && libPath.startsWith("/")) {
            //avoid things like "/C:/what/ever" on windows
            libPath = libPath.substring(1);
          }
          if (!libSet.contains(libPath)) {
            pathJoiner.add(libPath);
            libSet.add(libPath);
          }
        }
      }
      return pathJoiner.toString();
    }
  }

  public static String toValidJavaIdentifier(String javaIdentifier) {
    if (StringUtils.isBlank(javaIdentifier)) {
      throw new IllegalArgumentException("Expected not-blank identifier!");
    }

    javaIdentifier = StringUtils.deleteWhitespace(javaIdentifier)
            .replaceAll("\\W+", "_");

    if (!SourceVersion.isIdentifier(javaIdentifier) || SourceVersion.isKeyword(javaIdentifier)) {
      javaIdentifier = "a_" + javaIdentifier;
    }

    return javaIdentifier;
  }

  static String getTruncateSchemaForWarning(Schema schema) {
    if (schema == null) {
      return "null";
    }
    String schemaString = schema.toString();
    return (schemaString.length() > MAX_SCHEMA_LENGTH_IN_WARNING)
        ? schemaString.substring(0, MAX_SCHEMA_LENGTH_IN_WARNING) + "..."
        : schemaString;
  }
}
