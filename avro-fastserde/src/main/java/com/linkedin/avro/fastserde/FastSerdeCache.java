package com.linkedin.avro.fastserde;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;


/**
 * Fast avro serializer/deserializer cache. Stores generated and already compiled instances of serializers and
 * deserializers for future use.
 */
@SuppressWarnings("unchecked")
public final class FastSerdeCache {

  public static final String CLASSPATH = "avro.fast.serde.classpath";
  public static final String CLASSPATH_SUPPLIER = "avro.fast.serde.classpath.supplier";

  private static final Logger LOGGER = Logger.getLogger(FastSerdeCache.class);

  private static volatile FastSerdeCache _INSTANCE;

  private final ConcurrentHashMap<String, FastDeserializer<?>> fastSpecificRecordDeserializersCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, FastDeserializer<?>> fastGenericRecordDeserializersCache =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, FastSerializer<?>> fastSpecificRecordSerializersCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, FastSerializer<?>> fastGenericRecordSerializersCache =
      new ConcurrentHashMap<>();

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
            // Infer class path if no classpath specified.
            classPath = System.getProperty("java.class.path");
            String avroSchemaClassName = "org.apache.avro.Schema";
            try {
              Class avroSchemaClass = Class.forName(avroSchemaClassName);
              String avroLibLocation = avroSchemaClass.getProtectionDomain().getCodeSource().getLocation().getFile();
              // find all other libs
              File avroLibFile = new File(avroLibLocation).getParentFile();
              for (File lib : avroLibFile.listFiles()) {
                if (lib.getName().endsWith(".jar")) {
                  classPath += ":" + lib.getAbsolutePath();
                }
              }
              LOGGER.info("Inferred class path: " + classPath);
            } catch (ClassNotFoundException e) {
              throw new RuntimeException("Failed to find class: " + avroSchemaClassName);
            }
            _INSTANCE = new FastSerdeCache(classPath);
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

  /**
   * Generates if needed and returns specific-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema
   *            {@link Schema} of written data
   * @param readerSchema
   *            {@link Schema} intended to be used during deserialization
   * @return specific-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      deserializer = fastSpecificRecordDeserializersCache.putIfAbsent(schemaKey,
          new FastDeserializerWithAvroSpecificImpl<>(writerSchema, readerSchema));
      if (deserializer == null) {
        deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);
        CompletableFuture.supplyAsync(() -> buildSpecificDeserializer(writerSchema, readerSchema), executor)
            .thenAccept(d -> {
              fastSpecificRecordDeserializersCache.put(schemaKey, d);
            });
      }
    }

    return deserializer;
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastDeserializer}.
   *
   * @param writerSchema
   *            {@link Schema} of written data
   * @param readerSchema
   *            {@link Schema} intended to be used during deserialization
   * @return generic-class aware avro {@link FastDeserializer}
   */
  public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    FastDeserializer<?> deserializer = fastGenericRecordDeserializersCache.get(schemaKey);

    if (deserializer == null) {
      deserializer = fastGenericRecordDeserializersCache.putIfAbsent(schemaKey,
          new FastDeserializerWithAvroGenericImpl(writerSchema, readerSchema));
      if (deserializer == null) {
        deserializer = fastGenericRecordDeserializersCache.get(schemaKey);
        CompletableFuture.supplyAsync(() -> buildGenericDeserializer(writerSchema, readerSchema), executor)
            .thenAccept(d -> {
              fastGenericRecordDeserializersCache.put(schemaKey, d);
            });
      }
    }
    return deserializer;
  }

  /**
   * Generates if needed and returns specific-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @return specific-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema) {
    String schemaKey = getSchemaKey(schema, schema);
    FastSerializer<?> serializer = fastSpecificRecordSerializersCache.get(schemaKey);
    if (serializer == null) {
      serializer =
          fastSpecificRecordSerializersCache.putIfAbsent(schemaKey, new FastSerializerWithAvroSpecificImpl(schema));
      if (serializer == null) {
        serializer = fastSpecificRecordSerializersCache.get(schemaKey);
        CompletableFuture.supplyAsync(() -> buildSpecificSerializer(schema), executor).thenAccept(s -> {
          fastSpecificRecordSerializersCache.put(schemaKey, s);
        });
      }
    }

    return serializer;
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @return generic-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema) {
    String schemaKey = getSchemaKey(schema, schema);

    FastSerializer<?> serializer = fastGenericRecordSerializersCache.get(schemaKey);
    if (serializer == null) {
      serializer =
          fastGenericRecordSerializersCache.putIfAbsent(schemaKey, new FastSerializerWithAvroGenericImpl(schema));
      if (serializer == null) {
        serializer = fastGenericRecordSerializersCache.get(schemaKey);
        CompletableFuture.supplyAsync(() -> buildGenericSerializer(schema), executor).thenAccept(s -> {
          fastGenericRecordSerializersCache.put(schemaKey, s);
        });
      }
    }
    return serializer;
  }

  private String getSchemaKey(Schema writerSchema, Schema readerSchema) {
    return String.valueOf(Math.abs(Utils.getSchemaFingerprint(writerSchema))) + Math.abs(
        Utils.getSchemaFingerprint(readerSchema));
  }

  /**
   * This function will generate a fast specific deserializer, and it will throw exception if anything wrong happens.
   * This function can be used to verify whether current {@link FastSerdeCache} could generate proper fast deserializer.
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema
   * @return a fast deserializer
   */
  public FastDeserializer<?> buildFastSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    LOGGER.info("Start buildFastSpecificDeserializer for reader schema: [" + readerSchema + "] and write schema: ["
        + writerSchema + "]");

    FastSpecificDeserializerGenerator<?> generator =
        new FastSpecificDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated classes dir: " + classesDir + ", and generation of specific FastDeserializer"
        + " is done for reader schema: [" + readerSchema + "] and write schema: [" + writerSchema + "]");
    return generator.generateDeserializer();
  }

  /**
   * This function is used to generate a fast generic deserializer, and it will fail back to use
   * {@link SpecificDatumReader} if anything wrong happens.
   * @param writerSchema
   * @param readerSchema
   * @return
   */
  private FastDeserializer<?> buildSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
    try {
      return buildFastSpecificDeserializer(writerSchema, readerSchema);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.warn("Deserializer generation exception", e);
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
   * This function will generate a fast generic deserializer, and it will throw exception if anything wrong happens.
   * This function can be used to verify whether current {@link FastSerdeCache} could generate proper fast deserializer.
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema
   * @return a fast deserializer
   */
  public FastDeserializer<?> buildFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    LOGGER.info("Start buildFastGenericDeserializer for reader schema: [" + readerSchema + "] and write schema: ["
        + writerSchema + "]");

    FastGenericDeserializerGenerator<?> generator =
        new FastGenericDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated classes dir: " + classesDir + " and generation of generic FastDeserializer"
        + " is done for reader schema: [" + readerSchema + "] is done");
    return generator.generateDeserializer();
  }

  /**
   * This function is used to generate a fast generic deserializer, and it will fail back to use
   * {@link GenericDatumReader} if anything wrong happens.
   *
   * @param writerSchema
   * @param readerSchema
   * @return
   */
  private FastDeserializer<?> buildGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    try {
      return buildFastGenericDeserializer(writerSchema, readerSchema);
    } catch (FastDeserializerGeneratorException e) {
      LOGGER.warn("Deserializer generation exception:" + e);
    } catch (Exception e) {
      LOGGER.warn("Deserializer class instantiation exception:" + e);
    }

    return new FastDeserializer<Object>() {
      private DatumReader datumReader = new GenericDatumReader<>(writerSchema, readerSchema);

      @Override
      public Object deserialize(Object reuse, Decoder d) throws IOException {
        return datumReader.read(reuse, d);
      }
    };
  }

  public FastSerializer<?> buildFastSpecificSerializer(Schema schema) {
    // Defensive code
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      throw new FastDeserializerGeneratorException("Fast specific serializer is only supported in following Avro versions: " +
          Utils.getAvroVersionsSupportedForSerializer());
    }
    LOGGER.info("Start buildFastSpecificSerializer for schema: [" + schema + "]");
    FastSpecificSerializerGenerator<?> generator =
        new FastSpecificSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
    LOGGER.info(
        "Generated classes dir: " + classesDir + " and generation of specific FastSerializer" + " is done for schema: ["
            + schema + "] is done");
    return generator.generateSerializer();
  }

  private FastSerializer<?> buildSpecificSerializer(Schema schema) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast specific serializer for supported Avro versions.
      try {
        return buildFastSpecificSerializer(schema);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception", e);
      } catch (Exception e) {
        LOGGER.warn("Serializer class instantiation exception", e);
      }
    }

    return new FastSerializer<Object>() {
      private final DatumWriter datumWriter = new SpecificDatumWriter(schema);

      @Override
      public void serialize(Object data, Encoder e) throws IOException {
        datumWriter.write(data, e);
      }
    };
  }

  public FastSerializer<?> buildFastGenericSerializer(Schema schema) {
    // Defensive code
    if (!Utils.isSupportedAvroVersionsForSerializer()) {
      throw new FastDeserializerGeneratorException("Fast generic serializer is only supported in following avro versions:"
          + Utils.getAvroVersionsSupportedForSerializer());
    }
    LOGGER.info("Start buildFastGenericSerializer for schema: [" + schema + "]");
    FastGenericSerializerGenerator<?> generator =
        new FastGenericSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
    LOGGER.info(
        "Generated classes dir: " + classesDir + " and generation of generic FastSerializer" + " is done for schema: ["
            + schema + "] is done");
    return generator.generateSerializer();
  }

  private FastSerializer<?> buildGenericSerializer(Schema schema) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast generic serializer for supported Avro versions.
      try {
        return buildFastGenericSerializer(schema);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception", e);
      } catch (Exception e) {
        LOGGER.warn("Serializer class instantiation exception", e);
      }
    }

    return new FastSerializer<Object>() {
      private final DatumWriter datumWriter = new GenericDatumWriter(schema);

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

    public FastDeserializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema) {
      this.datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
    }

    @Override
    public V deserialize(V reuse, Decoder d) throws IOException {
      return datumReader.read(reuse, d);
    }
  }

  public static class FastDeserializerWithAvroGenericImpl<V> implements FastDeserializer<V> {
    private final GenericDatumReader<V> datumReader;

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema) {
      this.datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    }

    @Override
    public V deserialize(V reuse, Decoder d) throws IOException {
      return datumReader.read(reuse, d);
    }
  }

  public static class FastSerializerWithAvroSpecificImpl<V> implements FastSerializer<V> {
    private final SpecificDatumWriter<V> datumWriter;

    public FastSerializerWithAvroSpecificImpl(Schema schema) {
      this.datumWriter = new SpecificDatumWriter<>(schema);
    }

    @Override
    public void serialize(V data, Encoder e) throws IOException {
      datumWriter.write(data, e);
    }
  }

  public static class FastSerializerWithAvroGenericImpl<V> implements FastSerializer<V> {
    private final DatumWriter<V> datumWriter;

    public FastSerializerWithAvroGenericImpl(Schema schema) {
      this.datumWriter = new GenericDatumWriter<>(schema);
    }

    @Override
    public void serialize(V data, Encoder e) throws IOException {
      datumWriter.write(data, e);
    }
  }
}
