package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import static com.linkedin.avro.fastserde.Utils.*;


/**
 * Utilities used by both serialization and deserialization code.
 */
public abstract class FastSerdeBase {
  private static final Logger LOGGER = Logger.getLogger(FastSerdeBase.class);
  protected static final String SEP = "_";
  public static final String GENERATED_PACKAGE_NAME_PREFIX = "com.linkedin.avro.fastserde.generated.";

  /**
   * A repository of how many times a given name was used.
   * N.B.: Does not actually need to be threadsafe, but it is made so just for defensive coding reasons.
   */
  private final ConcurrentMap<String, AtomicInteger> counterPerName = new FastAvroConcurrentHashMap<>();
  private final String generatedSourcesPath;
  protected final String generatedPackageName;
  protected final JCodeModel codeModel = new JCodeModel();
  protected final boolean useGenericTypes;
  protected final SchemaAssistant schemaAssistant;
  protected final File destination;
  protected final ClassLoader classLoader;
  protected final String compileClassPath;
  protected JDefinedClass generatedClass;

  public FastSerdeBase(String description, boolean useGenericTypes, Class defaultStringClass, File destination, ClassLoader classLoader,
      String compileClassPath) {
    this.useGenericTypes = useGenericTypes;
    this.schemaAssistant = new SchemaAssistant(codeModel, useGenericTypes, defaultStringClass);
    this.destination = destination;
    this.classLoader = classLoader;
    this.compileClassPath = (null == compileClassPath ? "" : compileClassPath);
    this.generatedPackageName = GENERATED_PACKAGE_NAME_PREFIX + description + "." + AvroCompatibilityHelper.getRuntimeAvroVersion().name();
    this.generatedSourcesPath = generateSourcePathFromPackageName(generatedPackageName);
  }

  /**
   * A function to generate unique names, such as those of variables and functions, within the scope
   * of the this class instance (i.e. per serializer of a given schema or deserializer of a given
   * schema pair).
   *
   * @param prefix String to serve as a prefix for the unique name
   * @return a unique prefix composed of the {@param prefix} appended by a unique number
   */
  protected String getUniqueName(String prefix) {
    String uncapitalizedPrefix = StringUtils.uncapitalize(prefix);
    return uncapitalizedPrefix + nextUniqueInt(uncapitalizedPrefix);
  }

  private int nextUniqueInt(String name) {
    return counterPerName.computeIfAbsent(name, k -> new AtomicInteger(0)).getAndIncrement();
  }

  protected void ifCodeGen(JBlock parentBody, JExpression condition, Consumer<JBlock> thenClosure) {
    JConditional ifCondition = parentBody._if(condition);
    thenClosure.accept(ifCondition._then());
  }

  protected void ifCodeGen(JBlock parentBody, JExpression condition, Consumer<JBlock> thenClosure,
      Consumer<JBlock> elseClosure) {
    JConditional ifCondition = parentBody._if(condition);
    thenClosure.accept(ifCondition._then());
    elseClosure.accept(ifCondition._else());
  }

  protected JVar declareValueVar(final String name, final Schema schema, JBlock block) {
    return declareValueVar(name, schema, block, true, false, false);
  }

  protected JVar declareValueVar(final String name, final Schema schema, JBlock block, boolean abstractType, boolean rawType, boolean primitiveList) {
    if (SchemaAssistant.isComplexType(schema)) {
      return block.decl(schemaAssistant.classFromSchema(schema, abstractType, rawType, primitiveList), getUniqueName(name),
          JExpr._null());
    } else {
      throw new FastDeserializerGeneratorException("Only complex types allowed!");
    }
  }

  @SuppressWarnings("unchecked")
  protected Class compileClass(final String className, Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + generatedSourcesPath + className + ".java";
    LOGGER.info("Generated source file: " + filePath);

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    String compileClassPathForCurrentFile = Utils.inferCompileDependencies(compileClassPath, filePath, knownUsedFullyQualifiedClassNameSet);
    LOGGER.info("For source file: " + filePath + ", and the inferred compile class path: " + compileClassPathForCurrentFile);
    int compileResult;
    try {
      compileResult = compiler.run(null, null, null, "-cp", compileClassPathForCurrentFile, filePath);
    } catch (Exception e) {
      throw new FastSerdeGeneratorException("Unable to compile:" + className, e);
    }

    if (compileResult != 0) {
      throw new FastSerdeGeneratorException("Unable to compile:" + className);
    }

    return classLoader.loadClass(generatedPackageName + "." + className);
  }
}
