package com.linkedin.avro.fastserde;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.ColdGenericDatumReader;
import org.apache.avro.generic.ColdSpecificDatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(FastSerdeCache.class);

  private static volatile FastSerdeCache _INSTANCE;

  /**
   * Fast-avro will generate and load serializer and deserializer(SerDes) classes into metaspace during runtime.
   * During serialization and deserialization, fast-avro also leverages JIT compilation to boost the SerDes speed.
   * And JIT compilation code is saved in code cache.
   * Too much usage of metaspace and code cache will bring GC/OOM issue.
   *
   * We set a hard limit of the total number of SerDes classes generated and loaded by fast-avro.
   * By default, the limit is set to MAX_INT.
   * Fast-avro will fall back to regular avro after the limit is hit.
   * One could set the limit through {@link FastSerdeCache} constructors.
   * And get the limit by {@link #getGeneratedFastClassNumLimit}
   */
  private volatile int generatedFastClassNumLimit = Integer.MAX_VALUE;
  private AtomicInteger schemaNum = new AtomicInteger(0);

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

  /**
   *
   * @param executorService
   *            {@link Executor} used by serializer/deserializer compile threads
   * @param compileClassPathSupplier
   *            custom classpath {@link Supplier}
   * @param limit
   *            custom number {@link #generatedFastClassNumLimit}
   */
  public FastSerdeCache(Executor executorService, Supplier<String> compileClassPathSupplier, int limit) {
    this(executorService, compileClassPathSupplier);
    setGeneratedFastClassNumLimit(limit);
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
   * @param limit
   *            custom number {@link #generatedFastClassNumLimit}
   */
  public FastSerdeCache(int limit) {
    this();
    setGeneratedFastClassNumLimit(limit);
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
    return getDeSerializer(writerSchema, readerSchema, fastSpecificRecordDeserializersCache,
        FastDeserializerWithAvroSpecificImpl.class, "buildSpecificDeserializer");
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
    return getDeSerializer(writerSchema, readerSchema, fastGenericRecordDeserializersCache,
        FastDeserializerWithAvroGenericImpl.class, "buildFastGenericDeserializer");
  }

  /**
   * Generates if needed and returns specific-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @return specific-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastSpecificSerializer(Schema schema) {
    return getDeSerializer(schema, schema, fastSpecificRecordSerializersCache,
        FastSerializerWithAvroSpecificImpl.class, "buildSpecificSerializer");
  }

  /**
   * Generates if needed and returns generic-class aware avro {@link FastSerializer}.
   *
   * @param schema
   *            {@link Schema} of data to write
   * @return generic-class aware avro {@link FastSerializer}
   */
  public FastSerializer<?> getFastGenericSerializer(Schema schema) {
    return getDeSerializer(schema, schema, fastGenericRecordSerializersCache,
        FastSerializerWithAvroGenericImpl.class, "buildGenericSerializer");
  }

  private String getSchemaKey(Schema writerSchema, Schema readerSchema) {
    return String.valueOf(Math.abs(Utils.getSchemaFingerprint(writerSchema))) + Math.abs(
        Utils.getSchemaFingerprint(readerSchema));
  }

  /**
   * This function can get the total schema number limit for {@link FastSerdeCache}
   *
   * @return current schema number limit
   */
  public int getGeneratedFastClassNumLimit() {
    return generatedFastClassNumLimit;
  }

  /**
   * Set the total schema number limit for {@link FastSerdeCache}.
   * It is the total limit for fast Generic/Specific De/Serializer
   */
  private void setGeneratedFastClassNumLimit(int limit) {
    generatedFastClassNumLimit = limit;
  }

  /**
   * This function is a template to generate if needed and return specific/generic-class aware avro de/serializer
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema (for serializer, this can be null)
   * @param cache cache for specific/generic de/serializer
   * @param avroImplClass generator for regular avro de/serializer
   * @param buildFastDeSerializerFuncName the name of helper function to build specific/generic de/serializer
   *
   * @return a fast specific/generic de/serializer
   */
  private <T> T getDeSerializer(Schema writerSchema, Schema readerSchema, Map<String, T> cache, Class<?> avroImplClass,
      String buildFastDeSerializerFuncName){
    String schemaKey = getSchemaKey(writerSchema, readerSchema);
    T deserializer = cache.get(schemaKey);
    if (deserializer == null) {
      try {
        Constructor cs = avroImplClass.getConstructor(Schema.class, Schema.class);
        deserializer = cache.putIfAbsent(schemaKey, (T)cs.newInstance(writerSchema, readerSchema));
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (deserializer == null) {
        deserializer = cache.get(schemaKey);
        if (schemaNum.get() < generatedFastClassNumLimit) {
          try {
            Method
                buildDeSerializer = FastSerdeCache.class.getMethod(buildFastDeSerializerFuncName, Schema.class, Schema.class);
            CompletableFuture.supplyAsync(() -> {
              try {
                return (T)buildDeSerializer.invoke(_INSTANCE ,writerSchema, readerSchema);
              } catch (Exception e) {
                e.printStackTrace();
              }
              return null;
            }, executor).thenAccept(s -> {
              cache.put(schemaKey, s);
            });
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          LOGGER.warn("Generated fast de/serializer number hits " + generatedFastClassNumLimit + " limit\n" +
              "Total de/serializer used is " + schemaNum.get());
        }
        schemaNum.incrementAndGet();
      }
    }
    return deserializer;
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
    FastSpecificDeserializerGenerator<?> generator =
        new FastSpecificDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated class dir: {}, and generation of specific FastDeserializer is done for writer schema: "
            + "[\n{}\n] and reader schema: [\n{}\n]", classesDir, writerSchema.toString(true), readerSchema.toString(true));
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
   * This function will generate a fast generic deserializer, and it will throw exception if anything wrong happens.
   * This function can be used to verify whether current {@link FastSerdeCache} could generate proper fast deserializer.
   *
   * @param writerSchema writer schema
   * @param readerSchema reader schema
   * @return a fast deserializer
   */
  public FastDeserializer<?> buildFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
    FastGenericDeserializerGenerator<?> generator =
        new FastGenericDeserializerGenerator<>(writerSchema, readerSchema, classesDir, classLoader,
            compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated classes dir: {} and generation of generic FastDeserializer is done for writer schema: "
            + "[\n{}\n] and reader schema:[\n{}\n]", classesDir, writerSchema.toString(true), readerSchema.toString(true));
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
      LOGGER.warn("Deserializer generation exception when generating generic FastDeserializer for writer schema: [\n"
          + writerSchema.toString(true) + "\n] and reader schema:[\n" + readerSchema.toString(true) + "\n]", e);
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
      throw new FastDeserializerGeneratorException("Specific FastSerializer is only supported in following Avro versions: " +
          Utils.getAvroVersionsSupportedForSerializer());
    }
    FastSpecificSerializerGenerator<?> generator =
        new FastSpecificSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated classes dir: {} and generation of specific FastSerializer is done for schema: [\n{}\n]",
        classesDir, schema.toString(true));
    return generator.generateSerializer();
  }

  private FastSerializer<?> buildSpecificSerializer(Schema schema, Schema uselessSchema) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast specific serializer for supported Avro versions.
      try {
        return buildFastSpecificSerializer(schema);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception when generating specific FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
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
      throw new FastDeserializerGeneratorException("Generic FastSerializer is only supported in following avro versions:"
          + Utils.getAvroVersionsSupportedForSerializer());
    }
    FastGenericSerializerGenerator<?> generator =
        new FastGenericSerializerGenerator<>(schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
    LOGGER.info("Generated classes dir: {} and generation of generic FastSerializer is done for schema: [\n{}\n]",
        classesDir, schema.toString(true));
    return generator.generateSerializer();
  }

  private FastSerializer<?> buildGenericSerializer(Schema schema, Schema uselessSchema) {
    if (Utils.isSupportedAvroVersionsForSerializer()) {
      // Only build fast generic serializer for supported Avro versions.
      try {
        return buildFastGenericSerializer(schema);
      } catch (FastDeserializerGeneratorException e) {
        LOGGER.warn("Serializer generation exception when generating generic FastSerializer for schema: [\n{}\n]",
            schema.toString(true), e);
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
      this.datumReader = new ColdSpecificDatumReader<>(writerSchema, readerSchema);
    }

    @Override
    public V deserialize(V reuse, Decoder d) throws IOException {
      return datumReader.read(reuse, d);
    }
  }

  public static class FastDeserializerWithAvroGenericImpl<V> implements FastDeserializer<V> {
    private final GenericDatumReader<V> datumReader;

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema) {
      this.datumReader = new ColdGenericDatumReader<>(writerSchema, readerSchema);
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

    public FastSerializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema) {
      this(writerSchema);
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

    public FastSerializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema) {
      this(writerSchema);
    }

    @Override
    public void serialize(V data, Encoder e) throws IOException {
      datumWriter.write(data, e);
    }
  }
}
