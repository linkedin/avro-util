package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import static com.linkedin.avro.fastserde.Utils.*;


/**
 * TODO: refactor {@link FastSerializerGeneratorBase} and {@link FastDeserializerGeneratorBase} to eliminate the duplicate code.
 * @param <T> type of Datum to be operated on
 */
public abstract class FastSerializerGeneratorBase<T> extends FastSerdeBase {
  public static final String GENERATED_PACKAGE_NAME = "com.linkedin.avro.fastserde.generated.serialization."
      + AvroCompatibilityHelper.getRuntimeAvroVersion().name();
  public static final String GENERATED_SOURCES_PATH = generateSourcePathFromPackageName(GENERATED_PACKAGE_NAME);

  protected static final String SEP = "_";

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
    this.compileClassPath = (null == compileClassPath ? "" : compileClassPath);
    codeModel = new JCodeModel();
  }

  public static String getClassName(Schema schema, String description) {
    Long schemaId = Math.abs(Utils.getSchemaFingerprint(schema));
    String typeName = SchemaAssistant.getTypeName(schema);
    return typeName + SEP + description + "Serializer" + SEP + schemaId;
  }

  public abstract FastSerializer<T> generateSerializer();

  @SuppressWarnings("unchecked")
  protected Class<FastSerializer<T>> compileClass(final String className, Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java";
    LOGGER.info("Generated serializer source file: " + filePath);

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    String compileClassPathForCurrentFile = Utils.inferCompileDependencies(compileClassPath, filePath, knownUsedFullyQualifiedClassNameSet);
    LOGGER.info("For source file: " + filePath + ", and the inferred compile class path: " + compileClassPathForCurrentFile);
    int compileResult;
    try {
      compileResult = compiler.run(null, null, null, "-cp", compileClassPathForCurrentFile, filePath);
    } catch (Exception e) {
      throw new FastSerializerGeneratorException("Unable to compile:" + className, e);
    }

    if (compileResult != 0) {
      throw new FastSerializerGeneratorException("Unable to compile:" + className);
    }

    return (Class<FastSerializer<T>>) classLoader.loadClass(GENERATED_PACKAGE_NAME + "." + className);
  }
}
