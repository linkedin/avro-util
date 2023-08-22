package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.avro.fastserde.Utils.*;


/**
 * Utilities used by both serialization and deserialization code.
 */
public abstract class FastSerdeBase<T extends GenericData> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastSerdeBase.class);
  protected static final String SEP = "_";
  public static final String GENERATED_PACKAGE_NAME_PREFIX = "com.linkedin.avro.fastserde.generated.";

  private final Set<String> injectedSchemaFieldNames = new HashSet<>();
  private final Set<String> injectedConversionFieldNames = new HashSet<>();

  /**
   * A repository of how many times a given name was used.
   * N.B.: Does not actually need to be threadsafe, but it is made so just for defensive coding reasons.
   */
  private final ConcurrentMap<String, AtomicInteger> counterPerName = new FastAvroConcurrentHashMap<>();
  private final String generatedSourcesPath;
  protected final String generatedPackageName;
  protected final JCodeModel codeModel = new JCodeModel();
  protected final boolean useGenericTypes;
  protected final SchemaAssistant<T> schemaAssistant;
  protected final File destination;
  protected final ClassLoader classLoader;
  protected final String compileClassPath;
  /**
   * Contains information regarding conversion classes used by logical types feature.
   * In case of specific Avro class it is just its MODEL$ field.
   */
  protected final T modelData;
  protected JDefinedClass generatedClass;

  public FastSerdeBase(String description, boolean useGenericTypes, Class defaultStringClass, File destination, ClassLoader classLoader,
      String compileClassPath, T modelData, boolean isForSerializer) {
    this.useGenericTypes = useGenericTypes;
    this.schemaAssistant = new SchemaAssistant<>(codeModel, useGenericTypes, defaultStringClass, modelData, isForSerializer);
    this.destination = destination;
    this.classLoader = classLoader;
    this.compileClassPath = (null == compileClassPath ? "" : compileClassPath);
    this.modelData = modelData;
    this.generatedPackageName = GENERATED_PACKAGE_NAME_PREFIX + description + "." + AvroCompatibilityHelper.getRuntimeAvroVersion().name();
    this.generatedSourcesPath = generateSourcePathFromPackageName(generatedPackageName);
  }

  /**
   * A function to generate unique names, such as those of variables and functions, within the scope
   * of this class instance (i.e. per serializer of a given schema or deserializer of a given
   * schema pair).
   *
   * @param prefix String to serve as a prefix for the unique name
   * @return a unique prefix composed of the prefix appended by a unique number
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

  protected void injectConversionClasses(JMethod constructor) {
    if (Utils.isLogicalTypeSupported()) {
      JClass modelDataClass = codeModel.ref(useGenericTypes ? GenericData.class : SpecificData.class);
      JFieldVar modelDataField = generatedClass.field(JMod.PRIVATE | JMod.FINAL, modelDataClass, "modelData");
      JVar modelDataCtorParam = constructor.param(modelDataClass, modelDataField.name());
      constructor.body().assign(JExpr.refthis(modelDataField.name()), modelDataCtorParam);

      if (modelData != null) {
        modelData.getConversions().forEach(this::injectConversionFieldIfMissing);
      }
    }
  }

  protected JFieldRef getConversionRef(LogicalType logicalType) {
    final Conversion<?> conversion = (Conversion<?>) schemaAssistant.getConversion(logicalType);
    return injectConversionFieldIfMissing(conversion);
  }

  private JFieldRef injectConversionFieldIfMissing(Conversion<?> conversion) {
    final String conversionFieldName = Utils.toValidJavaIdentifier("conversion_" + conversion.getLogicalTypeName());

    if (injectedConversionFieldNames.add(conversionFieldName)) {
      generatedClass.field(JMod.PRIVATE | JMod.FINAL, conversion.getClass(), conversionFieldName,
              JExpr._new(codeModel.ref(conversion.getClass())));
    }

    return JExpr.refthis(conversionFieldName);
  }

  protected JFieldRef injectLogicalTypeSchema(Schema schema) {
    String schemaFieldName = toLogicalTypeSchemaFieldName(schema);
    if (injectedSchemaFieldNames.add(schemaFieldName)) {
      generatedClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class, schemaFieldName,
              codeModel.ref(Schema.class).staticInvoke("parse").arg(schema.toString()));
    }

    return JExpr.refthis(schemaFieldName);
  }

  protected String toLogicalTypeSchemaFieldName(Schema schema) {
    long schemaFingerprint = Utils.getSchemaFingerprint(schema);
    return ("logicalTypeSchema_" + schemaFingerprint).replace('-', '_');
  }

  protected boolean logicalTypeEnabled(Schema schema) {
    return schemaAssistant.logicalTypeEnabled(schema);
  }

  protected Class compileClass(final String className, Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + generatedSourcesPath + className + ".java";

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (null == compiler) {
      /*
       * If the above function returns null, it is very likely that the env setting: "JAVA_HOME" is not being setup properly.
       */
      throw new FastSerdeGeneratorException("Couldn't locate java compiler at runtime, please double check your env "
          + "setting for 'JAVA_HOME', and here is the value for 'System.getProperty(\"java.home\")': " + System.getProperty("java.home"));
    }
    String compileClassPathForCurrentFile = Utils.inferCompileDependencies(compileClassPath, filePath, knownUsedFullyQualifiedClassNameSet);
    int compileResult;
    try {
      /*
       * Disable sharedNameTable in runtime complication
       *
       * The SharedNameTable was introduced to speed up Java complication by using soft references
       * to avoid re-allocations. However, in fast-avro runtime compilation, sharedNameTable brings
       * severe Memory and GC issue. When fast-avro needed to process a large number of different
       * schemas, SharedNameTable objects will consume huge memory and cannot be freed.
       *
       * SharedNameTable should be disabled for runtime compilation by "-XDuseUnsharedTable" config.
       * The memory issue by SharedNameTable does not exist in Java 11 (tested JDK-11_0_5-zulu
       * and JDK-11_0_5-zing_19_12_100_0_1), thus the change can be reverted in java 11.
       * Keeping this config also does not bring any downgrade.
       *
       */
      LOGGER.info("Starting compilation for the generated source file: {} ", filePath);
      LOGGER.debug("The inferred compile class path for file: {} : {}", filePath, compileClassPathForCurrentFile);
      compileResult = compiler.run(null, null, null, "-cp", compileClassPathForCurrentFile, filePath, "-XDuseUnsharedTable");
    } catch (Exception e) {
      throw new FastSerdeGeneratorException("Unable to compile:" + className + " from source file: " + filePath, e);
    }

    if (compileResult != 0) {
      throw new FastSerdeGeneratorException("Unable to compile:" + className + " from source file: " + filePath);
    } else {
      LOGGER.info("Successfully compiled class {} defined at source file: {}", className, filePath);
    }

    return classLoader.loadClass(generatedPackageName + "." + className);
  }
}
