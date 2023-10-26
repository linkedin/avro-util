package com.linkedin.avro.fastserde;

import static com.linkedin.avro.fastserde.Utils.getSchemaFingerprint;
import static com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSchemaFullName;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fast avro serializer/deserializer cache. Stores generated and already compiled instances of serializers and
 * deserializers for future use.
 */
@SuppressWarnings("unchecked")
public final class FastSerdeCache {

  public static final String CLASSPATH = "avro.fast.serde.classpath";
  public static final String CLASSPATH_SUPPLIER = "avro.fast.serde.classpath.supplier";
  public static final String FAIL_FAST = "avro.fast.serde.failfast";
  public static final String FAIL_FAST_SUPPLIER = "avro.fast.serde.failfast.supplier";

  private static final Logger LOGGER = LoggerFactory.getLogger(FastSerdeCache.class);

  private static volatile FastSerdeCache _INSTANCE;

  private final Map<String, FastDeserializer<?>> fastSpecificRecordDeserializersCache =
      new FastAvroConcurrentHashMap<>();
  private final Map<String, FastDeserializer<?>> fastGenericRecordDeserializersCache =
      new FastAvroConcurrentHashMap<>();

  private final Map<String, FastSerializer<?>> fastSpecificRecordSerializersCache =
      new FastAvroConcurrentHashMap<>();
  private final Map<String, FastSerializer<?>> fastGenericRecordSerializersCache =
      new FastAvroConcurrentHashMap<>();

  private final Executor executor;

  private final File classesDir;
  private final ClassLoader classLoader;

  private final String compileClassPath;
  private final boolean failFast;

  /**
   *
   * @param compileClassPathSupplier
   *            custom classpath {@link Supplier}
   */
  public FastSerdeCache(Supplier<String> compileClassPathSupplier) {
    this(null, compileClassPathSupplier != null ? compileClassPathSupplier.get() : null);
  }

  /**
   *
   * @param executorService
   *            {@link Executor} used by serializer/deserializer compile threads
   * @param compileClassPathSupplier
   *            custom classpath {@link Supplier}
   */
  public FastSerdeCache(Executor executorService, Supplier<String> compileClassPathSupplier) {
    this(executorService, compileClassPathSupplier.get());
  }

  public FastSerdeCache(String compileClassPath) {
    this(null, compileClassPath);
  }

  /**
   *
   * @param executorService
   *            customized {@link Executor} used by serializer/deserializer compile threads
   */
  public FastSerdeCache(Executor executorService) {
    this(executorService, (String) null);
  }

  public FastSerdeCache(Executor executorService, String compileClassPath) {
    this(executorService, compileClassPath, false);
  }

  /**
   * @param executorService
   *            customized {@link Executor} used by serializer/deserializer compile threads
   * @param compileClassPath
   *            custom classpath as string
   * @param failFast
   *            'true' indicates generating always-failing Fast(de-)Serializer if fast-serde class couldn't be generated
   *            (e.g. due to compilation error)
   */
  public FastSerdeCache(Executor executorService, String compileClassPath, boolean failFast) {
    this.executor = executorService != null ? executorService : getDefaultExecutor();

    try {
      Path classesPath = Files.createTempDirectory("generated");
      classesDir = classesPath.toFile();
      classLoader =
              URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()}, FastSerdeCache.class.getClassLoader());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.compileClassPath = compileClassPath;
    this.failFast = failFast;
  }

  /**
   * Gets default {@link FastSerdeCache} instance. Default instance classpath can be customized via
   * {@value #CLASSPATH} or {@value #CLASSPATH_SUPPLIER} system properties.
   *
   * @return default {@link FastSerdeCache} instance
   */
  public static FastSerdeCache getDefaultInstance() {
    if (_INSTANCE == null) {
      synchronized (FastSerdeCache.class) {
        if (_INSTANCE == null) {
          String compileClassPath = resolveSystemProperty(CLASSPATH_SUPPLIER, CLASSPATH);
          String failFast = resolveSystemProperty(FAIL_FAST_SUPPLIER, FAIL_FAST);
          /*
           * The fast-class generator will figure out the compile dependencies during fast-class generation.
           * `compileClassPath` extends above findings, e.g. may provide jar with custom conversions for logical types.
           */
          _INSTANCE = new FastSerdeCache(null, compileClassPath, Boolean.parseBoolean(failFast));
        }
      }
    }
    return _INSTANCE;
  }

  private static String resolveSystemProperty(String supplierPropertyName, String propertyName) {
    String supplierClassName = System.getProperty(supplierPropertyName);

    if (StringUtils.isNotBlank(supplierClassName)) {
      Supplier<String> supplier = null;
      try {
        Class<?> supplierClass = Class.forName(supplierClassName);
        if (Supplier.class.isAssignableFrom(supplierClass) && String.class.equals(
                ((ParameterizedType) supplierClass.getGenericSuperclass()).getActualTypeArguments()[0])) {

          supplier = (Supplier<String>) supplierClass.getConstructor().newInstance();
        } else {
          LOGGER.warn("Supplier must be subtype of java.util.function.Supplier: " + supplierClassName);
        }
      } catch (ReflectiveOperationException e) {
        LOGGER.warn("Unable to instantiate supplier: " + supplierClassName, e);
      }

      return supplier != null ? supplier.get() : null;
    }

    return System.getProperty(propertyName);
  }

  public static boolean isSupportedForFastDeserializer(Schema.Type readerSchemaType) {
    return readerSchemaType.equals(Schema.Type.RECORD) || readerSchemaType.equals(Schema.Type.MAP)
        || readerSchemaType.equals(Schema.Type.ARRAY);
  }

  public static boolean isSupportedForFastSerializer(Schema.Type schemaType) {
    return schemaType.equals(Schema.Type.RECORD) || schemaType.equals(Schema.Type.MAP) || schemaType.equals(
        Schema.Type.ARRAY);
  }

  public static boolean isFastDeserializer(FastDeserializer deserializer) {
    return deserializer.isBackedByGeneratedClass();
  }

  /**
   * @see #getFastSpecificDeserializer(Schema, Schema, SpecificData)
   */
  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    return getFastSpecificDeserializer(writerSchema, readerSchema, null);
  }

  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    return getFastSpecificDeserializer(writerSchema, readerSchema, modelData, null);
  }

  /**
   * Generates if needed and returns specific-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema
   *            {@link Schema} of written data
   * @param readerSchema
   *            {@link Schema} intended to be used during deserialization
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @param customization
   *            Provides customized logic during de-serialization
   * @return specific-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema,
      SpecificData modelData, DatumReaderCustomization customization) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      AtomicBoolean fastDeserializerMissingInCache = new AtomicBoolean(false);
      deserializer = fastSpecificRecordDeserializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastDeserializerMissingInCache.set(true);
            return new FastSerdeUtils.FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema, modelData, customization, false);
          });

      if (fastDeserializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildSpecificDeserializer(writerSchema, readerSchema, modelData, customization), executor)
            .thenAccept(d -> fastSpecificRecordDeserializersCache.put(schemaKey, d));
      }
    }

    return deserializer;
  }

  /**
   * @see #getFastGenericDeserializer(Schema, Schema, GenericData)
   */
  public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    return getFastGenericDeserializer(writerSchema, readerSchema, null);
  }

  public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    return getFastGenericDeserializer(writerSchema, readerSchema, modelData, null);
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema
   *            {@link Schema} of written data
   * @param readerSchema
   *            {@link Schema} intended to be used during deserialization
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @param customization
   *            Provides customized logic during de-serialization
   * @return generic-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema,
      GenericData modelData, DatumReaderCustomization customization) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastGenericRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      AtomicBoolean fastDeserializerMissingInCache = new AtomicBoolean(false);
      deserializer = fastGenericRecordDeserializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastDeserializerMissingInCache.set(true);
            return new FastSerdeUtils.FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema, modelData, customization, false);
          });

      if (fastDeserializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildGenericDeserializer(writerSchema, readerSchema, modelData, customization), executor)
            .thenAccept(d -> fastGenericRecordDeserializersCache.put(schemaKey, d));
      }
    }
    return deserializer;
  }

  /**
   * @see #getFastSpecificSerializer(Schema, SpecificData, DatumWriterCustomization)
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema) {
    return getFastSpecificSerializer(schema, null, null);
  }

  /**
   * Generates if needed and returns specific-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @param customization
   *            Provides customized logic during serialization
   * @return specific-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema, SpecificData modelData, DatumWriterCustomization customization) {
    String schemaKey = getSchemaKey(schema, schema);
    FastSerializer<?> serializer = fastSpecificRecordSerializersCache.get(schemaKey);

    if (serializer == null) {
      AtomicBoolean fastSerializerMissingInCache = new AtomicBoolean(false);
      serializer = fastSpecificRecordSerializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastSerializerMissingInCache.set(true);
            return new FastSerdeUtils.FastSerializerWithAvroSpecificImpl<>(schema, modelData, customization, false);
          });

      if (fastSerializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildSpecificSerializer(schema, modelData, customization), executor)
            .thenAccept(s -> fastSpecificRecordSerializersCache.put(schemaKey, s));
      }
    }

    return serializer;
  }

  /**
   * @see #getFastGenericSerializer(Schema, GenericData, DatumWriterCustomization)
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema) {
    return getFastGenericSerializer(schema, null, null);
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @param modelData
   *            Passes additional information e.g. conversion classes not available in the schema
   * @param customization
   *            Provides customized logic during serialization
   * @return generic-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema, GenericData modelData, DatumWriterCustomization customization) {
    String schemaKey = getSchemaKey(schema, schema);
    FastSerializer<?> serializer = fastGenericRecordSerializersCache.get(schemaKey);

    if (serializer == null) {
      AtomicBoolean fastSerializerMissingInCache = new AtomicBoolean(false);
      serializer = fastGenericRecordSerializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastSerializerMissingInCache.set(true);
            return new FastSerdeUtils.FastSerializerWithAvroGenericImpl<>(schema, modelData, customization, false);
          });

      if (fastSerializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildGenericSerializer(schema, modelData, customization), executor)
            .thenAccept(s -> fastGenericRecordSerializersCache.put(schemaKey, s));
      }
    }

    return serializer;
  }

  /**
   * @see #getFastSpecificDeserializerAsync(Schema, Schema, SpecificData, DatumReaderCustomization)
   */
  public CompletableFuture<FastDeserializer<?>> getFastSpecificDeserializerAsync(Schema writerSchema, Schema readerSchema) {
    return getFastSpecificDeserializerAsync(writerSchema, readerSchema, null, null);
  }

  /**
   * Asynchronously generates if needed and returns specific-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema {@link Schema} of written data
   * @param readerSchema {@link Schema} intended to be used during deserialization
   * @param modelData Passes additional information e.g. conversion classes not available in the schema
   * @param customization Provides customized logic during de-serialization
   * @return {@link CompletableFuture} which contains specific-class aware avro {@link FastDeserializer}
   */
  public CompletableFuture<FastDeserializer<?>> getFastSpecificDeserializerAsync(Schema writerSchema,
      Schema readerSchema, SpecificData modelData, DatumReaderCustomization customization) {
    return getFastDeserializerAsync(writerSchema, readerSchema, fastSpecificRecordDeserializersCache,
        () -> buildSpecificDeserializer(writerSchema, readerSchema, modelData, customization));
  }

  /**
   * @see #getFastGenericDeserializerAsync(Schema, Schema, GenericData, DatumReaderCustomization)
   */
  public CompletableFuture<FastDeserializer<?>> getFastGenericDeserializerAsync(Schema writerSchema, Schema readerSchema) {
    return getFastGenericDeserializerAsync(writerSchema, readerSchema, null, null);
  }

  /**
   * Asynchronously generates if needed and returns generic-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema {@link Schema} of written data
   * @param readerSchema {@link Schema} intended to be used during deserialization
   * @param modelData Passes additional information e.g. conversion classes not available in the schema
   * @param customization Provides customized logic during de-serialization
   * @return {@link CompletableFuture} which contains generic-class aware avro {@link FastDeserializer}
   */
  public CompletableFuture<FastDeserializer<?>> getFastGenericDeserializerAsync(Schema writerSchema, Schema readerSchema,
      GenericData modelData, DatumReaderCustomization customization) {
    return getFastDeserializerAsync(writerSchema, readerSchema, fastGenericRecordDeserializersCache,
        () -> buildGenericDeserializer(writerSchema, readerSchema, modelData, customization));
  }

  private CompletableFuture<FastDeserializer<?>> getFastDeserializerAsync(Schema writerSchema, Schema readerSchema,
      Map<String, FastDeserializer<?>> fastDeserializerCache, Supplier<FastDeserializer<?>> fastDeserializerSupplier) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastDeserializerCache.get(schemaKey);
    return deserializer != null && isFastDeserializer(deserializer) ? CompletableFuture.completedFuture(deserializer)
        : CompletableFuture.supplyAsync(fastDeserializerSupplier, executor)
            .thenApply(d -> {
              fastDeserializerCache.put(schemaKey, d);
              return d;
            });
  }

  private static String getSchemaKey(Schema writerSchema, Schema readerSchema) {
    return String.valueOf(Math.abs(getSchemaFingerprint(writerSchema))) + Math.abs(
        getSchemaFingerprint(readerSchema));
  }

  /**
   * @see #buildFastSpecificDeserializer(Schema, Schema, SpecificData)
   */
  public FastDeserializer<?> buildFastSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    return buildFastSpecificDeserializer(writerSchema, readerSchema, null);
  }

  /**
   * This function will generate a fast specific deserializer, and it will throw exception if anything wrong happens.
   * This function can be used to verify whether current {@link FastSerdeCache} could generate proper fast deserializer.
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema
   * @param modelData Passes additional information e.g. conversion classes not available in the schema
   * @return a fast deserializer
   */
  public FastDeserializer<?> buildFastSpecificDeserializer(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    FastSpecificDeserializerGenerator<?> generator =
        new FastSpecificDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader, compileClassPath, modelData);
    FastDeserializer<?> fastDeserializer = generator.generateDeserializer();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Generated classes dir: {} and generation of specific FastDeserializer is done for writer schema of type: {} with fingerprint: {}"
              + " and content: [\n{}\n] and reader schema of type: {} with fingerprint: {} and content: [\n{}\n]", classesDir, getSchemaFullName(writerSchema),
              writerSchema.toString(true), getSchemaFingerprint(writerSchema), getSchemaFullName(readerSchema), getSchemaFingerprint(readerSchema),
              readerSchema.toString(true));
    } else {
      LOGGER.info("Generated classes dir: {} and generation of specific FastDeserializer is done for writer schema of type: {} with fingerprint: {}"
              + " and reader schema of type: {} with fingerprint: {}", classesDir, getSchemaFullName(writerSchema), getSchemaFingerprint(writerSchema),
              getSchemaFullName(readerSchema), getSchemaFingerprint(readerSchema));
    }

    return fastDeserializer;
  }

  /**
   * This function is used to generate a fast generic deserializer, and it will fail back to use
   * {@link SpecificDatumReader} if anything wrong happens.
   * @param writerSchema
   * @param readerSchema
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @return
   */
  private FastDeserializer<?> buildSpecificDeserializer(Schema writerSchema, Schema readerSchema,
      SpecificData modelData, DatumReaderCustomization customization) {
    try {
      return buildFastSpecificDeserializer(writerSchema, readerSchema, modelData);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.error("Deserializer generation exception when generating specific FastDeserializer for writer schema: "
              + "[\n{}\n] and reader schema: [\n{}\n]", writerSchema.toString(true), readerSchema.toString(true), e);
    } catch (Exception e) {
      LOGGER.error("Deserializer class instantiation exception", e);
    }

    return new FastSerdeUtils.FastDeserializerWithAvroSpecificImpl(writerSchema, readerSchema, modelData, customization, failFast, true);
  }

  /**
   * @see #buildFastGenericDeserializer(Schema, Schema, GenericData)
   */
  public FastDeserializer<?> buildFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    return buildFastGenericDeserializer(writerSchema, readerSchema, null);
  }

  /**
   * This function will generate a fast generic deserializer, and it will throw exception if anything wrong happens.
   * This function can be used to verify whether current {@link FastSerdeCache} could generate proper fast deserializer.
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema
   * @param modelData Provides additional information not available in the schema, e.g. conversion classes
   * @return a fast deserializer
   */
  public FastDeserializer<?> buildFastGenericDeserializer(Schema writerSchema, Schema readerSchema,
      GenericData modelData) {
    FastGenericDeserializerGenerator<?> generator =
        new FastGenericDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader, compileClassPath, modelData);

    FastDeserializer<?> fastDeserializer = generator.generateDeserializer();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Generated classes dir: {} and generation of generic FastDeserializer is done for writer schema of type: {} with fingerprint: {}"
              + " and content: [\n{}\n] and reader schema of type: {} with fingerprint: {} and content: [\n{}\n]", classesDir, getSchemaFullName(writerSchema),
              writerSchema.toString(true), getSchemaFingerprint(writerSchema), getSchemaFullName(readerSchema), getSchemaFingerprint(readerSchema),
              readerSchema.toString(true));
    } else {
      LOGGER.info("Generated classes dir: {} and generation of generic FastDeserializer is done for writer schema of type: {} with fingerprint: {}"
              + " and reader schema of type: {} with fingerprint: {}", classesDir, getSchemaFullName(writerSchema), getSchemaFingerprint(writerSchema),
              getSchemaFullName(readerSchema), getSchemaFingerprint(readerSchema));
    }

    return fastDeserializer;
  }

  /**
   * This function is used to generate a fast generic deserializer, and it will fail back to use
   * {@link GenericDatumReader} if anything wrong happens.
   *
   * @param writerSchema
   * @param readerSchema
   * @param modelData Provides additional information not available in the schema, e.g. conversion classes
   * @return
   */
  private FastDeserializer<?> buildGenericDeserializer(Schema writerSchema, Schema readerSchema,
      GenericData modelData, DatumReaderCustomization customization) {
    try {
      return buildFastGenericDeserializer(writerSchema, readerSchema, modelData);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.error("Deserializer generation exception when generating generic FastDeserializer for writer schema: [\n"
          + writerSchema.toString(true) + "\n] and reader schema:[\n" + readerSchema.toString(true) + "\n]", e);
    } catch (Exception e) {
      LOGGER.error("Deserializer class instantiation exception:", e);
    }

    return new FastSerdeUtils.FastDeserializerWithAvroGenericImpl(writerSchema, readerSchema, modelData, customization, failFast, true);
  }

  public FastSerializer<?> buildFastSpecificSerializer(Schema schema, SpecificData modelData) {
    // Defensive code
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      throw new FastDeserializerGeneratorException("Specific FastSerializer is only supported in following Avro versions: " +
          Utils.getAvroVersionsSupportedForSerializer());
    }
    FastSpecificSerializerGenerator<?> generator =
        new FastSpecificSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath, modelData);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Generated classes dir: {} and generation of specific FastSerializer is done for schema of type: {}" +
              " and fingerprint: {} and content: [\n{}\n]", classesDir, getSchemaFullName(schema), getSchemaFingerprint(schema),
              schema.toString(true));
    } else {
      LOGGER.info("Generated classes dir: {} and generation of specific FastSerializer is done for schema of type: {}" +
              " and fingerprint: {}", classesDir, getSchemaFullName(schema), getSchemaFingerprint(schema));
    }

    return generator.generateSerializer();
  }

  private FastSerializer<?> buildSpecificSerializer(Schema schema, SpecificData modelData, DatumWriterCustomization customization) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast specific serializer for supported Avro versions.
      try {
        return buildFastSpecificSerializer(schema, modelData);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.error("Serializer generation exception when generating specific FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
      } catch (Exception e) {
        LOGGER.error("Serializer class instantiation exception", e);
      }
    }

    return new FastSerdeUtils.FastSerializerWithAvroSpecificImpl<>(schema, modelData, customization, failFast, true);
  }

  public FastSerializer<?> buildFastGenericSerializer(Schema schema) {
    return buildFastGenericSerializer(schema, null);
  }

  public FastSerializer<?> buildFastGenericSerializer(Schema schema, GenericData modelData) {
    // Defensive code
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      throw new FastDeserializerGeneratorException("Generic FastSerializer is only supported in following avro versions:"
          + Utils.getAvroVersionsSupportedForSerializer());
    }
    FastGenericSerializerGenerator<?> generator =
        new FastGenericSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath, modelData);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Generated classes dir: {} and generation of generic FastSerializer is done for schema of type: {}" +
              " and fingerprint: {} and content: [\n{}\n]", classesDir, getSchemaFullName(schema), getSchemaFingerprint(schema),
              schema.toString(true));
    } else {
      LOGGER.info("Generated classes dir: {} and generation of generic FastSerializer is done for schema of type: {}" +
              " and fingerprint: {}", classesDir, getSchemaFullName(schema), getSchemaFingerprint(schema));
    }

    return generator.generateSerializer();
  }

  private FastSerializer<?> buildGenericSerializer(Schema schema, GenericData modelData, DatumWriterCustomization customization) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast generic serializer for supported Avro versions.
      try {
        return buildFastGenericSerializer(schema, modelData);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.error("Serializer generation exception when generating generic FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
      } catch (Exception e) {
        LOGGER.error("Serializer class instantiation exception", e);
      }
    }

    return new FastSerdeUtils.FastSerializerWithAvroGenericImpl<>(schema, modelData, customization, failFast, true);
  }

  public boolean isFailFast() {
    return failFast;
  }

  private Executor getDefaultExecutor() {
    return Executors.newFixedThreadPool(2, new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("avro-fastserde-compile-thread-" + threadNumber.getAndIncrement());
        return thread;
      }
    });
  }
}
