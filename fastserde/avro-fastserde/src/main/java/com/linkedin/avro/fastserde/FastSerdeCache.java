package com.linkedin.avro.fastserde;

import static com.linkedin.avro.fastserde.Utils.getSchemaFingerprint;
import static com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSchemaFullName;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.generic.ColdGenericDatumReader;
import org.apache.avro.generic.ColdSpecificDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;


/**
 * Fast avro serializer/deserializer cache. Stores generated and already compiled instances of serializers and
 * deserializers for future use.
 */
@SuppressWarnings("unchecked")
public final class FastSerdeCache {

  public static final String CLASSPATH = "avro.fast.serde.classpath";
  public static final String CLASSPATH_SUPPLIER = "avro.fast.serde.classpath.supplier";

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

  private Executor executor;

  private File classesDir;
  private ClassLoader classLoader;

  private Optional<String> compileClassPath;

  /**
   *
   * @param compileClassPathSupplier
   *            custom classpath {@link Supplier}
   */
  public FastSerdeCache(Supplier<String> compileClassPathSupplier) {
    this(compileClassPathSupplier != null ? compileClassPathSupplier.get() : null);
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
    this();
    this.compileClassPath = Optional.ofNullable(compileClassPath);
  }

  /**
   *
   * @param executorService
   *            customized {@link Executor} used by serializer/deserializer compile threads
   * @param compileClassPath
   *            custom classpath as string
   */
  public FastSerdeCache(Executor executorService, String compileClassPath) {
    this(executorService);
    this.compileClassPath = Optional.ofNullable(compileClassPath);
  }

  /**
   *
   * @param executorService
   *            customized {@link Executor} used by serializer/deserializer compile threads
   */
  public FastSerdeCache(Executor executorService) {
    this.executor = executorService != null ? executorService : getDefaultExecutor();

    try {
      Path classesPath = Files.createTempDirectory("generated");
      classesDir = classesPath.toFile();
      classLoader =
          URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()}, FastSerdeCache.class.getClassLoader());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.compileClassPath = Optional.empty();
  }

  private FastSerdeCache() {
    this((Executor) null);
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
          String classPath = System.getProperty(CLASSPATH);
          String classpathSupplierClassName = System.getProperty(CLASSPATH_SUPPLIER);
          if (classpathSupplierClassName != null) {
            Supplier<String> classpathSupplier = null;
            try {
              Class<?> classPathSupplierClass = Class.forName(classpathSupplierClassName);
              if (Supplier.class.isAssignableFrom(classPathSupplierClass) && String.class.equals(
                  ((ParameterizedType) classPathSupplierClass.getGenericSuperclass()).getActualTypeArguments()[0])) {

                classpathSupplier = (Supplier<String>) classPathSupplierClass.newInstance();
              } else {
                LOGGER.warn(
                    "classpath supplier must be subtype of java.util.function.Supplier: " + classpathSupplierClassName);
              }
            } catch (ReflectiveOperationException e) {
              LOGGER.warn("unable to instantiate classpath supplier: " + classpathSupplierClassName, e);
            }
            _INSTANCE = new FastSerdeCache(classpathSupplier);
          } else if (classPath != null) {
            _INSTANCE = new FastSerdeCache(classPath);
          } else {
            /**
             * The fast-class generator will figure out the compile dependencies during fast-class generation.
             */
            _INSTANCE = new FastSerdeCache("");
          }
        }
      }
    }
    return _INSTANCE;
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
    return !(deserializer instanceof FastDeserializerWithAvroSpecificImpl
        || deserializer instanceof FastDeserializerWithAvroGenericImpl);
  }

  /**
   * @see #getFastSpecificDeserializer(Schema, Schema, SpecificData)
   */
  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    return getFastSpecificDeserializer(writerSchema, readerSchema, null);
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
   * @return specific-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      AtomicBoolean fastDeserializerMissingInCache = new AtomicBoolean(false);
      deserializer = fastSpecificRecordDeserializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastDeserializerMissingInCache.set(true);
            return new FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema, modelData);
          });

      if (fastDeserializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildSpecificDeserializer(writerSchema, readerSchema, modelData), executor)
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

  /**
   * Generates if needed and returns generic-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema
   *            {@link Schema} of written data
   * @param readerSchema
   *            {@link Schema} intended to be used during deserialization
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @return generic-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastGenericRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      AtomicBoolean fastDeserializerMissingInCache = new AtomicBoolean(false);
      deserializer = fastGenericRecordDeserializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastDeserializerMissingInCache.set(true);
            return new FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema, modelData);
          });

      if (fastDeserializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildGenericDeserializer(writerSchema, readerSchema, modelData), executor)
            .thenAccept(d -> fastGenericRecordDeserializersCache.put(schemaKey, d));
      }
    }
    return deserializer;
  }
  
  /**
   * @see #getFastSpecificSerializer(Schema, SpecificData)
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema) {
    return getFastSpecificSerializer(schema, null);
  }

  /**
   * Generates if needed and returns specific-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @param modelData
   *            Provides additional information not available in the schema, e.g. conversion classes
   * @return specific-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema, SpecificData modelData) {
    String schemaKey = getSchemaKey(schema, schema);
    FastSerializer<?> serializer = fastSpecificRecordSerializersCache.get(schemaKey);

    if (serializer == null) {
      AtomicBoolean fastSerializerMissingInCache = new AtomicBoolean(false);
      serializer = fastSpecificRecordSerializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastSerializerMissingInCache.set(true);
            return new FastSerializerWithAvroSpecificImpl<>(schema, modelData);
          });

      if (fastSerializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildSpecificSerializer(schema, modelData), executor)
            .thenAccept(s -> fastSpecificRecordSerializersCache.put(schemaKey, s));
      }
    }

    return serializer;
  }

  /**
   * @see #getFastGenericSerializer(Schema, GenericData)
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema) {
    return getFastGenericSerializer(schema, null);
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @param modelData
   *            Passes additional information e.g. conversion classes not available in the schema
   * @return generic-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema, GenericData modelData) {
    String schemaKey = getSchemaKey(schema, schema);
    FastSerializer<?> serializer = fastGenericRecordSerializersCache.get(schemaKey);

    if (serializer == null) {
      AtomicBoolean fastSerializerMissingInCache = new AtomicBoolean(false);
      serializer = fastGenericRecordSerializersCache.computeIfAbsent(
          schemaKey,
          k -> {
            fastSerializerMissingInCache.set(true);
            return new FastSerializerWithAvroGenericImpl<>(schema, modelData);
          });

      if (fastSerializerMissingInCache.get()) {
        CompletableFuture.supplyAsync(() -> buildGenericSerializer(schema, modelData), executor)
            .thenAccept(s -> fastGenericRecordSerializersCache.put(schemaKey, s));
      }
    }

    return serializer;
  }

  /**
   * @see #getFastSpecificDeserializerAsync(Schema, Schema, SpecificData)
   */
  public CompletableFuture<FastDeserializer<?>> getFastSpecificDeserializerAsync(Schema writerSchema, Schema readerSchema) {
    return getFastSpecificDeserializerAsync(writerSchema, readerSchema, null);
  }

  /**
   * Asynchronously generates if needed and returns specific-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema {@link Schema} of written data
   * @param readerSchema {@link Schema} intended to be used during deserialization
   * @param modelData Passes additional information e.g. conversion classes not available in the schema
   * @return {@link CompletableFuture} which contains specific-class aware avro {@link FastDeserializer}
   */
  public CompletableFuture<FastDeserializer<?>> getFastSpecificDeserializerAsync(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    return getFastDeserializerAsync(writerSchema, readerSchema, fastSpecificRecordDeserializersCache,
        () -> buildSpecificDeserializer(writerSchema, readerSchema, modelData));
  }

  /**
   * @see #getFastGenericDeserializerAsync(Schema, Schema, GenericData)
   */
  public CompletableFuture<FastDeserializer<?>> getFastGenericDeserializerAsync(Schema writerSchema, Schema readerSchema) {
    return getFastGenericDeserializerAsync(writerSchema, readerSchema);
  }

  /**
   * Asynchronously generates if needed and returns generic-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema {@link Schema} of written data
   * @param readerSchema {@link Schema} intended to be used during deserialization
   * @param modelData Passes additional information e.g. conversion classes not available in the schema
   * @return {@link CompletableFuture} which contains generic-class aware avro {@link FastDeserializer}
   */
  public CompletableFuture<FastDeserializer<?>> getFastGenericDeserializerAsync(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    return getFastDeserializerAsync(writerSchema, readerSchema, fastGenericRecordDeserializersCache,
        () -> buildGenericDeserializer(writerSchema, readerSchema, modelData));
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
        new FastSpecificDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElse(null), modelData);
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
  private FastDeserializer<?> buildSpecificDeserializer(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
    try {
      return buildFastSpecificDeserializer(writerSchema, readerSchema, modelData);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.warn("Deserializer generation exception when generating specific FastDeserializer for writer schema: "
              + "[\n{}\n] and reader schema: [\n{}\n]", writerSchema.toString(true), readerSchema.toString(true), e);
    } catch (Exception e) {
      LOGGER.warn("Deserializer class instantiation exception", e);
    }

    return new FastDeserializer<Object>() {
      private DatumReader datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);

      @Override
      public Object deserialize(Object reuse, Decoder d) throws IOException {
        return datumReader.read(reuse, d);
      }
    };
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
  public FastDeserializer<?> buildFastGenericDeserializer(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    FastGenericDeserializerGenerator<?> generator =
        new FastGenericDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElse(null), modelData);

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
  private FastDeserializer<?> buildGenericDeserializer(Schema writerSchema, Schema readerSchema, GenericData modelData) {
    try {
      return buildFastGenericDeserializer(writerSchema, readerSchema, modelData);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.warn("Deserializer generation exception when generating generic FastDeserializer for writer schema: [\n"
          + writerSchema.toString(true) + "\n] and reader schema:[\n" + readerSchema.toString(true) + "\n]", e);
    } catch (Exception e) {
      LOGGER.warn("Deserializer class instantiation exception:" + e);
    }

    return new FastDeserializer<Object>() {
      private DatumReader datumReader = new GenericDatumReader<>(writerSchema, readerSchema, modelData);

      @Override
      public Object deserialize(Object reuse, Decoder d) throws IOException {
        return datumReader.read(reuse, d);
      }
    };
  }

  public FastSerializer<?> buildFastSpecificSerializer(Schema schema) {
    return buildFastSpecificSerializer(schema, null);
  }

  public FastSerializer<?> buildFastSpecificSerializer(Schema schema, SpecificData modelData) {
    // Defensive code
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      throw new FastDeserializerGeneratorException("Specific FastSerializer is only supported in following Avro versions: " +
          Utils.getAvroVersionsSupportedForSerializer());
    }
    FastSpecificSerializerGenerator<?> generator =
        new FastSpecificSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElse(null), modelData);

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

  private FastSerializer<?> buildSpecificSerializer(Schema schema, SpecificData modelData) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast specific serializer for supported Avro versions.
      try {
        return buildFastSpecificSerializer(schema, modelData);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception when generating specific FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
      } catch (Exception e) {
        LOGGER.warn("Serializer class instantiation exception", e);
      }
    }

    return new FastSerializer<Object>() {
      private final DatumWriter datumWriter = AvroCompatibilityHelper.newSpecificDatumWriter(schema,
              modelData != null ? modelData : SpecificData.get());

      @Override
      public void serialize(Object data, Encoder e) throws IOException {
        datumWriter.write(data, e);
      }
    };
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
        new FastGenericSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElse(null), modelData);

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

  private FastSerializer<?> buildGenericSerializer(Schema schema, GenericData modelData) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast generic serializer for supported Avro versions.
      try {
        return buildFastGenericSerializer(schema, modelData);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception when generating generic FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
      } catch (Exception e) {
        LOGGER.warn("Serializer class instantiation exception", e);
      }
    }

    return new FastSerializer<Object>() {
      private final DatumWriter datumWriter = AvroCompatibilityHelper.newGenericDatumWriter(schema,
              modelData != null ? modelData : GenericData.get());

      @Override
      public void serialize(Object data, Encoder e) throws IOException {
        datumWriter.write(data, e);
      }
    };
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

  public static class FastDeserializerWithAvroSpecificImpl<V> implements FastDeserializer<V> {
    private final SpecificDatumReader<V> datumReader;

    public FastDeserializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema, SpecificData modelData) {
      this.datumReader = ColdSpecificDatumReader.of(writerSchema, readerSchema, modelData);
    }

    @Override
    public V deserialize(V reuse, Decoder d) throws IOException {
      return datumReader.read(reuse, d);
    }
  }

  public static class FastDeserializerWithAvroGenericImpl<V> implements FastDeserializer<V> {
    private final GenericDatumReader<V> datumReader;

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema, GenericData modelData) {
      this.datumReader = ColdGenericDatumReader.of(writerSchema, readerSchema, modelData);
    }

    @Override
    public V deserialize(V reuse, Decoder d) throws IOException {
      return datumReader.read(reuse, d);
    }
  }

  public static class FastSerializerWithAvroSpecificImpl<V> implements FastSerializer<V> {
    private final DatumWriter<V> datumWriter;

    public FastSerializerWithAvroSpecificImpl(Schema schema) {
      this(schema, null);
    }

    public FastSerializerWithAvroSpecificImpl(Schema schema, SpecificData modelData) {
      this.datumWriter = AvroCompatibilityHelper.newSpecificDatumWriter(schema,
              modelData != null ? modelData : SpecificData.get());
    }

    @Override
    public void serialize(V data, Encoder e) throws IOException {
      datumWriter.write(data, e);
    }
  }

  public static class FastSerializerWithAvroGenericImpl<V> implements FastSerializer<V> {
    private final DatumWriter<V> datumWriter;

    public FastSerializerWithAvroGenericImpl(Schema schema, GenericData modelData) {
      this.datumWriter = AvroCompatibilityHelper.newGenericDatumWriter(schema,
              modelData != null ? modelData : GenericData.get());
    }

    @Override
    public void serialize(V data, Encoder e) throws IOException {
      datumWriter.write(data, e);
    }
  }
}
