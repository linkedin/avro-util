package com.linkedin.avro.fastserde;

import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import static com.linkedin.avro.fastserde.Utils.*;


public abstract class FastSerializerGeneratorBase<T> {
  public static final String GENERATED_PACKAGE_NAME = "com.linkedin.avro.fastserde.serialization.generated";
  public static final String GENERATED_SOURCES_PATH = generateSourcePathFromPackageName(GENERATED_PACKAGE_NAME);

  private static final AtomicInteger UNIQUE_ID_BASE = new AtomicInteger(0);

  private static final Logger LOGGER = Logger.getLogger(FastSerializerGeneratorBase.class);

  protected final Schema schema;
  protected JCodeModel codeModel;
  protected JDefinedClass serializerClass;
  private File destination;
  private ClassLoader classLoader;
  private String compileClassPath;

  FastSerializerGeneratorBase(Schema schema, File destination, ClassLoader classLoader, String compileClassPath) {
    this.schema = schema;
    this.destination = destination;
    this.classLoader = classLoader;
    this.compileClassPath = compileClassPath;
    codeModel = new JCodeModel();
  }

  public static String getClassName(Schema schema, String description) {
    Long schemaId = Math.abs(Utils.getSchemaFingerprint(schema));
    if (Schema.Type.RECORD.equals(schema.getType())) {
      return schema.getName() + description + "Serializer" + "_" + schemaId;
    } else if (Schema.Type.ARRAY.equals(schema.getType())) {
      return "Array" + description + "Serializer" + "_" + schemaId;
    } else if (Schema.Type.MAP.equals(schema.getType())) {
      return "Map" + description + "Serializer" + "_" + schemaId;
    }
    throw new FastSerializerGeneratorException("Unsupported return type: " + schema.getType());
  }

  protected static String getVariableName(String name) {
    return StringUtils.uncapitalize(name) + nextUniqueInt();
  }

  protected static int nextUniqueInt() {
    return UNIQUE_ID_BASE.getAndIncrement();
  }

  public abstract FastSerializer<T> generateSerializer();

  @SuppressWarnings("unchecked")
  protected Class<FastSerializer<T>> compileClass(final String className) throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java";
    LOGGER.info("Generated serializer source file: " + filePath);

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int compileResult;
    if (compileClassPath != null) {
      compileResult = compiler.run(null, null, null, "-cp", compileClassPath, filePath);
    } else {
      compileResult = compiler.run(null, null, null, filePath);
    }

    if (compileResult != 0) {
      throw new FastSerializerGeneratorException("unable to compile:" + className);
    }

    return (Class<FastSerializer<T>>) classLoader.loadClass(GENERATED_PACKAGE_NAME + "." + className);
  }
}
