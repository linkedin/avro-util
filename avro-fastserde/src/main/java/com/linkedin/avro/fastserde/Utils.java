package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroVersion;
import com.linkedin.avro.compatibility.SchemaNormalization;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.jboss.util.collection.WeakIdentityHashMap;


public class Utils {
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER = new ArrayList<>();
  private static final List<AvroVersion> AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER = new ArrayList<>();

  // Cache the mapping between Schema and the corresponding fingerprint
  private static final Map<Schema, Long> SCHEMA_IDS_CACHE = new ConcurrentHashMap<>();

  // Schema#hashCode is not performance efficient in Avro 1.4. It has been improved
  // by caching hashcode in the later version. Change to use WeakIdentityHashMap for
  // Avro 1.4 helps to improve the cache lookup performance. However, it requires
  // the client to resuse the schame instance.
  // TODO - LRU Cache with size limit
  private static final Map<Schema, Long> SCHEMA_IDS_CACHE_AVRO_14 = new WeakIdentityHashMap();

  private static final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

  private Utils() {
  }

  public static boolean isAvro14() {
    return AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_4);
  }

  public static boolean isSupportedAvroVersionsForDeserializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());
  }

  public static boolean isSupportedAvroVersionsForSerializer() {
    return AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.contains(AvroCompatibilityHelper.getRuntimeAvroVersion());

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
   * This function will produce a fingerprint for the provided schema.
   * @param schema a schema
   * @return fingerprint for the given schema
   */
  public static Long getSchemaFingerprint(Schema schema) {
    Long schemaId = null;
    if (isAvro14()) {
      // lookup cache and read lock
      reentrantReadWriteLock.readLock().lock();
      try {
        schemaId = SCHEMA_IDS_CACHE_AVRO_14.get(schema);
      } finally {
        reentrantReadWriteLock.readLock().unlock();
      }
      // update cache and write lock
      if (schemaId == null) {
        try {
          reentrantReadWriteLock.writeLock().lock();
          schemaId = SchemaNormalization.parsingFingerprint64(schema);
          SCHEMA_IDS_CACHE_AVRO_14.put(schema, schemaId);
        } finally {
          reentrantReadWriteLock.writeLock().unlock();
        }
      }
    } else {
      schemaId = SCHEMA_IDS_CACHE.get(schema);
      if (schemaId == null) {
        schemaId = SchemaNormalization.parsingFingerprint64(schema);
        SCHEMA_IDS_CACHE.put(schema, schemaId);
      }
    }
    return schemaId;
  }

  static {
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_4);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_DESERIALIZER.add(AvroVersion.AVRO_1_8);

    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_4);
    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_7);
    AVRO_VERSIONS_SUPPORTED_FOR_SERIALIZER.add(AvroVersion.AVRO_1_8);
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
    Set<String> libSet = Arrays.stream(existingCompileClasspath.split(":")).collect(Collectors.toSet());
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

      StringBuilder sb = new StringBuilder(existingCompileClasspath);
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
          if (!libSet.contains(libPath)) {
            sb.append(":").append(libPath);
            libSet.add(libPath);
          }
        }
      }
      return sb.toString();
    }
  }
}
