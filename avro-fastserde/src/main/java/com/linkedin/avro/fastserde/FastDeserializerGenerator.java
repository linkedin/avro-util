package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDoLoop;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JStatement;
import com.sun.codemodel.JSwitch;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;
import com.sun.codemodel.JWhileLoop;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastDeserializerGenerator.class);
  private static final String DECODER = "decoder";
  private static final String VAR_NAME_FOR_REUSE = "reuse";

  /**
   * This is sometimes passed into the reuse parameter,
   * and Avro treats null as a sentinel value indicating that it should
   * instantiate a new object instead of re-using.
   */
  private static final Supplier<JExpression> EMPTY_SUPPLIER = () -> JExpr._null();

  private JMethod constructor;
  private Map<Long, Schema> schemaMap = new HashMap<>();
  private Map<Long, JVar> schemaVarMap = new HashMap<>();
  private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
  private Map<String, JMethod> skipMethodMap = new HashMap<>();
  private Map<JMethod, Set<Class<? extends Exception>>> exceptionFromMethodMap = new HashMap<>();

  FastDeserializerGenerator(boolean useGenericTypes, Schema writer, Schema reader, File destination,
      ClassLoader classLoader, String compileClassPath) {
    super(useGenericTypes, writer, reader, destination, classLoader, compileClassPath);
  }

  public FastDeserializer<T> generateDeserializer() {
    String className = getClassName(writer, reader, useGenericTypes ? "Generic" : "Specific");
    JPackage classPackage = codeModel._package(generatedPackageName);

    try {
      generatedClass = classPackage._class(className);

      JVar readerSchemaVar = generatedClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class, "readerSchema");
      constructor = generatedClass.constructor(JMod.PUBLIC);
      JVar constructorParam = constructor.param(Schema.class, "readerSchema");
      constructor.body().assign(JExpr.refthis(readerSchemaVar.name()), constructorParam);

      Schema aliasedWriterSchema = writer;
      /**
       * {@link Schema.applyAliases} is not working correctly in avro-1.4 since there is a bug in this function:
       * {@literal Schema#getFieldAlias}.
       **/
      if (!Utils.isAvro14()) {
        aliasedWriterSchema = Schema.applyAliases(writer, reader);
      }
      Symbol resolvingGrammar = new ResolvingGrammarGenerator().generate(aliasedWriterSchema, reader);
      FieldAction fieldAction = FieldAction.fromValues(aliasedWriterSchema.getType(), true, resolvingGrammar);

      if (useGenericTypes) {
        registerSchema(aliasedWriterSchema, readerSchemaVar);
      }

      JClass readerSchemaClass = schemaAssistant.classFromSchema(reader);
      JClass writerSchemaClass = schemaAssistant.classFromSchema(aliasedWriterSchema);

      generatedClass._implements(codeModel.ref(FastDeserializer.class).narrow(writerSchemaClass));
      JMethod deserializeMethod = generatedClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

      JBlock topLevelDeserializeBlock = new JBlock();

      final Supplier<JExpression> reuseSupplier = () -> JExpr.direct(VAR_NAME_FOR_REUSE);
      switch (aliasedWriterSchema.getType()) {
        case RECORD:
          processRecord(readerSchemaVar, aliasedWriterSchema.getName(), aliasedWriterSchema, reader,
              topLevelDeserializeBlock, fieldAction, JBlock::_return, reuseSupplier);
          break;
        case ARRAY:
          processArray(readerSchemaVar, "array", aliasedWriterSchema, reader, topLevelDeserializeBlock, fieldAction,
              JBlock::_return, reuseSupplier);
          break;
        case MAP:
          processMap(readerSchemaVar, "map", aliasedWriterSchema, reader, topLevelDeserializeBlock, fieldAction,
              JBlock::_return, reuseSupplier);
          break;
        default:
          throw new FastDeserializerGeneratorException(
              "Incorrect top-level writer schema: " + aliasedWriterSchema.getType());
      }

      if (schemaAssistant.getExceptionsFromStringable().isEmpty()) {
        assignBlockToBody(deserializeMethod, topLevelDeserializeBlock);
      } else {
        JTryBlock tryBlock = deserializeMethod.body()._try();
        assignBlockToBody(tryBlock, topLevelDeserializeBlock);

        for (Class<? extends Exception> classException : schemaAssistant.getExceptionsFromStringable()) {
          JCatchBlock catchBlock = tryBlock._catch(codeModel.ref(classException));
          JVar exceptionVar = catchBlock.param("e");
          catchBlock.body()._throw(JExpr._new(codeModel.ref(AvroRuntimeException.class)).arg(exceptionVar));
        }
      }

      deserializeMethod._throws(codeModel.ref(IOException.class));
      deserializeMethod.param(readerSchemaClass, VAR_NAME_FOR_REUSE);
      deserializeMethod.param(Decoder.class, DECODER);

      Class<FastDeserializer<T>> clazz = compileClass(className, schemaAssistant.getUsedFullyQualifiedClassNameSet());
      return clazz.getConstructor(Schema.class).newInstance(reader);
    } catch (JClassAlreadyExistsException e) {
      throw new FastDeserializerGeneratorException("Class: " + className + " already exists");
    } catch (Exception e) {
      throw new FastDeserializerGeneratorException(e);
    }
  }

  private void processComplexType(JVar fieldSchemaVar, String name, Schema schema, Schema readerFieldSchema,
      JBlock methodBody, FieldAction action, BiConsumer<JBlock, JExpression> putExpressionIntoParent,
      Supplier<JExpression> reuseSupplier) {
    switch (schema.getType()) {
      case RECORD:
        processRecord(fieldSchemaVar, schema.getName(), schema, readerFieldSchema, methodBody, action,
            putExpressionIntoParent, reuseSupplier);
        break;
      case ARRAY:
        processArray(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent,
            reuseSupplier);
        break;
      case MAP:
        processMap(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent,
            reuseSupplier);
        break;
      case UNION:
        processUnion(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent,
            reuseSupplier);
        break;
      default:
        throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
    }
  }

  private void processSimpleType(Schema schema, Schema readerSchema, JBlock methodBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putExpressionIntoParent, Supplier<JExpression> reuseSupplier) {
    switch (schema.getType()) {
      case ENUM:
        processEnum(schema, methodBody, action, putExpressionIntoParent);
        break;
      case FIXED:
        processFixed(schema, methodBody, action, putExpressionIntoParent, reuseSupplier);
        break;
      default:
        // to preserve reader string specific options use reader field schema
        if (action.getShouldRead() && readerSchema != null && Schema.Type.STRING.equals(readerSchema.getType())) {
          processPrimitive(readerSchema, methodBody, action, putExpressionIntoParent, reuseSupplier);
        } else {
          processPrimitive(schema, methodBody, action, putExpressionIntoParent, reuseSupplier);
        }
        break;
    }
  }

  private void processRecord(JVar recordSchemaVar, String recordName, final Schema recordWriterSchema,
      final Schema recordReaderSchema, JBlock parentBody, FieldAction recordAction,
      BiConsumer<JBlock, JExpression> putRecordIntoParent, Supplier<JExpression> reuseSupplier) {

    ListIterator<Symbol> actionIterator = actionIterator(recordAction);

    if (methodAlreadyDefined(recordWriterSchema, recordAction.getShouldRead())) {
      JMethod method = getMethod(recordWriterSchema, recordAction.getShouldRead());
      updateActualExceptions(method);
      JExpression readingExpression = JExpr.invoke(method).arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
      if (recordAction.getShouldRead()) {
        putRecordIntoParent.accept(parentBody, readingExpression);
      } else {
        parentBody.add((JStatement) readingExpression);
      }

      // seek through actionIterator
      for (Schema.Field field : recordWriterSchema.getFields()) {
        FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
        if (action.getSymbol() == END_SYMBOL) {
          break;
        }
      }
      if (!recordAction.getShouldRead()) {
        return;
      }
      // seek through actionIterator also for default values
      Set<String> fieldNamesSet =
          recordWriterSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
      for (Schema.Field readerField : recordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
        }
      }
      return;
    }

    JMethod method = createMethod(recordWriterSchema, recordAction.getShouldRead());

    Set<Class<? extends Exception>> exceptionsOnHigherLevel = schemaAssistant.getExceptionsFromStringable();
    schemaAssistant.resetExceptionsFromStringable();

    if (recordAction.getShouldRead()) {
      putRecordIntoParent.accept(parentBody, JExpr.invoke(method).arg(reuseSupplier.get()).arg(JExpr.direct(DECODER)));
    } else {
      parentBody.invoke(method).arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
    }

    final JBlock methodBody = method.body();

    final JVar result;
    if (recordAction.getShouldRead()) {
      JClass recordClass = schemaAssistant.classFromSchema(recordWriterSchema);
      result = methodBody.decl(recordClass, recordName);

      JExpression reuseVar = JExpr.direct(VAR_NAME_FOR_REUSE);
      JClass indexedRecordClass = codeModel.ref(IndexedRecord.class);
      JInvocation newRecord = JExpr._new(schemaAssistant.classFromSchema(recordWriterSchema, false));
      if (useGenericTypes) {
        JExpression recordSchema = schemaVarMap.get(Utils.getSchemaFingerprint(recordWriterSchema));
        newRecord = newRecord.arg(recordSchema);
        JInvocation finalNewRecordInvocation = newRecord;

        ifCodeGen(methodBody,
            // Check whether schema matches
            reuseVar.ne(JExpr._null()).
                cand(reuseVar._instanceof(indexedRecordClass)).
                cand(
                    // The following statement will produce the equality check by "==", so that the comparison is
                    // not trying to compare the schema content, but just the reference, which should be very fast.
                    JExpr.invoke(JExpr.cast(indexedRecordClass, reuseVar), "getSchema").eq(recordSchema)),
            thenBlock -> thenBlock.assign(result, JExpr.cast(indexedRecordClass, reuseVar)),
            elseBlock -> elseBlock.assign(result, finalNewRecordInvocation)
        );
      } else {
        JInvocation finalNewRecordInvocation = newRecord;
        ifCodeGen(methodBody,
            reuseVar.ne(JExpr._null()),
            thenBlock -> thenBlock.assign(result, JExpr.cast(recordClass, reuseVar)),
            elseBlock -> elseBlock.assign(result, finalNewRecordInvocation)
        );
      }
    } else {
      result = null;
    }
    for (Schema.Field field : recordWriterSchema.getFields()) {
      FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
      if (action.getSymbol() == END_SYMBOL) {
        break;
      }

      Schema readerFieldSchema = null;
      JVar fieldSchemaVar = null;
      BiConsumer<JBlock, JExpression> putExpressionInRecord = null;
      Supplier<JExpression> fieldReuseSupplier = EMPTY_SUPPLIER;
      if (action.getShouldRead()) {
        Schema.Field readerField = recordReaderSchema.getField(field.name());
        readerFieldSchema = readerField.schema();
        final int readerFieldPos = readerField.pos();
        putExpressionInRecord =
            (block, expression) -> block.invoke(result, "put").arg(JExpr.lit(readerFieldPos)).arg(expression);
        if (useGenericTypes) {
          fieldSchemaVar = declareSchemaVar(field.schema(), field.name(),
              recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
        }
        fieldReuseSupplier = () -> result.invoke("get").arg(JExpr.lit(readerFieldPos));
      }
      if (SchemaAssistant.isComplexType(field.schema())) {
        processComplexType(fieldSchemaVar, field.name(), field.schema(), readerFieldSchema, methodBody, action,
            putExpressionInRecord, fieldReuseSupplier);
      } else {
        processSimpleType(field.schema(), readerFieldSchema, methodBody, action, putExpressionInRecord, fieldReuseSupplier);
      }
    }

    // Handle default values
    if (recordAction.getShouldRead()) {
      Set<String> fieldNamesSet =
          recordWriterSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
      for (Schema.Field readerField : recordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
          JVar schemaVar = null;
          if (useGenericTypes) {
            schemaVar = declareSchemaVariableForRecordField(readerField.name(), readerField.schema(), recordSchemaVar);
          }
          JExpression value = parseDefaultValue(readerField.schema(), readerField.defaultValue(), methodBody, schemaVar,
              readerField.name());
          methodBody.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(value);
        }
      }
    }

    if (recordAction.getShouldRead()) {
      methodBody._return(result);
    }
    exceptionFromMethodMap.put(method, schemaAssistant.getExceptionsFromStringable());
    schemaAssistant.setExceptionsFromStringable(exceptionsOnHigherLevel);
    updateActualExceptions(method);
  }

  private void updateActualExceptions(JMethod method) {
    Set<Class<? extends Exception>> exceptionFromMethod = exceptionFromMethodMap.get(method);
    if (exceptionFromMethod != null) {
      for (Class<? extends Exception> exceptionClass : exceptionFromMethod) {
        method._throws(exceptionClass);
        schemaAssistant.getExceptionsFromStringable().add(exceptionClass);
      }
    }
  }

  private JExpression parseDefaultValue(Schema schema, JsonNode defaultValue, JBlock body, JVar schemaVar,
      String fieldName) {
    Schema.Type schemaType = schema.getType();
    // The default value of union is of the first defined type
    if (Schema.Type.UNION.equals(schemaType)) {
      schema = schema.getTypes().get(0);
      schemaType = schema.getType();
      if (useGenericTypes) {
        JInvocation optionSchemaExpression = schemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(0));
        schemaVar = declareSchemaVar(schema, fieldName, optionSchemaExpression);
      }
    }
    // And default value of null is always null
    if (Schema.Type.NULL.equals(schemaType)) {
      return JExpr._null();
    }

    if (SchemaAssistant.isComplexType(schema)) {
      JClass defaultValueClass = schemaAssistant.classFromSchema(schema, false);
      JInvocation valueInitializationExpr = JExpr._new(defaultValueClass);

      JVar valueVar;
      switch (schemaType) {
        case RECORD:
          if (useGenericTypes) {
            valueInitializationExpr = valueInitializationExpr.arg(getSchemaExpr(schema));
          }
          valueVar =
              body.decl(defaultValueClass, getUniqueName("default" + schema.getName()), valueInitializationExpr);
          // Avro-1.4 depends on an old jackson-mapper-asl-1.4.2, which requires the following typecast.
          for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) defaultValue).getFields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> subFieldEntry = it.next();
            Schema.Field subField = schema.getField(subFieldEntry.getKey());

            JVar fieldSchemaVar = null;
            if (useGenericTypes) {
              fieldSchemaVar = declareSchemaVariableForRecordField(subField.name(), subField.schema(), schemaVar);
            }
            JExpression fieldValue =
                parseDefaultValue(subField.schema(), subFieldEntry.getValue(), body, fieldSchemaVar, subField.name());
            body.invoke(valueVar, "put").arg(JExpr.lit(subField.pos())).arg(fieldValue);
          }
          break;
        case ARRAY:
          JVar elementSchemaVar = null;
          if (useGenericTypes) {
            valueInitializationExpr =
                valueInitializationExpr.arg(JExpr.lit(defaultValue.size())).arg(getSchemaExpr(schema));
            elementSchemaVar =
                declareSchemaVar(schema.getElementType(), "defaultElementSchema", schemaVar.invoke("getElementType"));
          }

          valueVar = body.decl(defaultValueClass, getUniqueName("defaultArray"), valueInitializationExpr);

          for (JsonNode arrayElementValue : defaultValue) {
            JExpression arrayElementExpression =
                parseDefaultValue(schema.getElementType(), arrayElementValue, body, elementSchemaVar, "arrayValue");
            body.invoke(valueVar, "add").arg(arrayElementExpression);
          }
          break;
        case MAP:
          JVar mapValueSchemaVar = null;
          if (useGenericTypes) {
            mapValueSchemaVar =
                declareSchemaVar(schema.getValueType(), "defaultMapValueSchema", schemaVar.invoke("getValueType"));
          }

          valueVar = body.decl(defaultValueClass, getUniqueName("defaultMap"), valueInitializationExpr);

          // Avro-1.4 depends on an old jackson-mapper-asl-1.4.2, which requires the following typecast.
          for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) defaultValue).getFields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> mapEntry = it.next();
            JExpression mapKeyExpr;
            if (SchemaAssistant.hasStringableKey(schema)) {
              mapKeyExpr = JExpr._new(schemaAssistant.findStringClass(schema)).arg(mapEntry.getKey());
            } else {
              mapKeyExpr = JExpr._new(codeModel.ref(Utf8.class)).arg(mapEntry.getKey());
            }
            JExpression mapEntryValueExpression =
                parseDefaultValue(schema.getValueType(), mapEntry.getValue(), body, mapValueSchemaVar, "mapElement");
            body.invoke(valueVar, "put").arg(mapKeyExpr).arg(mapEntryValueExpression);
          }
          break;
        default:
          throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
      }
      return valueVar;
    } else {
      switch (schemaType) {
        case ENUM:
          return schemaAssistant.getEnumValueByName(schema, JExpr.lit(defaultValue.getTextValue()),
              getSchemaExpr(schema));
        case FIXED:
          JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
          for (char b : defaultValue.getTextValue().toCharArray()) {
            fixedBytesArray.add(JExpr.lit((byte) b));
          }
          // For specific Fixed type, Avro-1.4 will only generate the class with default constructor without params
          JClass fixedClass = schemaAssistant.classFromSchema(schema);
          if (useGenericTypes) {
            JInvocation newFixedExpr = null;
            if (Utils.isAvro14()) {
              newFixedExpr = JExpr._new(fixedClass).arg(fixedBytesArray);
            } else {
              newFixedExpr = JExpr._new(fixedClass).arg(getSchemaExpr(schema)).arg(fixedBytesArray);
            }
            return newFixedExpr;
          } else {
            JVar fixed = body.decl(fixedClass, getUniqueName(schema.getName()));
            JInvocation newFixedExpr = JExpr._new(fixedClass);
            body.assign(fixed, newFixedExpr);
            body.invoke(fixed, "bytes").arg(fixedBytesArray);
            return fixed;
          }
        case BYTES:
          JArray bytesArray = JExpr.newArray(codeModel.BYTE);
          for (byte b : defaultValue.getTextValue().getBytes()) {
            bytesArray.add(JExpr.lit(b));
          }
          return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
        case STRING:
          return schemaAssistant.getStringableValue(schema, JExpr.lit(defaultValue.getTextValue()));
        case INT:
          return JExpr.lit(defaultValue.getIntValue());
        case LONG:
          return JExpr.lit(defaultValue.getLongValue());
        case FLOAT:
          return JExpr.lit((float) defaultValue.getDoubleValue());
        case DOUBLE:
          return JExpr.lit(defaultValue.getDoubleValue());
        case BOOLEAN:
          return JExpr.lit(defaultValue.getBooleanValue());
        case NULL:
        default:
          throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
      }
    }
  }

  private void processUnion(JVar unionSchemaVar, final String name, final Schema unionSchema,
      final Schema readerUnionSchema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent, Supplier<JExpression> reuseSupplier) {
    JVar unionIndex = body.decl(codeModel.INT, getUniqueName("unionIndex"), JExpr.direct(DECODER + ".readIndex()"));
    JSwitch switchBlock = body._switch(unionIndex);
    for (int i = 0; i < unionSchema.getTypes().size(); i++) {
      Schema optionSchema = unionSchema.getTypes().get(i);
      Schema readerOptionSchema = null;
      int readerOptionUnionBranchIndex = -1;
      FieldAction unionAction;
      JBlock caseBody = switchBlock._case(JExpr.lit(i)).body();

      if (Schema.Type.NULL.equals(optionSchema.getType())) {
        caseBody.directStatement(DECODER + ".readNull();");
        caseBody._break();
        continue;
      }

      if (action.getShouldRead()) {
        // The reader's union could be re-ordered, so we need to find the one that matches.
        for (int j = 0; j < readerUnionSchema.getTypes().size(); j++) {
          Schema potentialReaderSchema = readerUnionSchema.getTypes().get(j);
          // Avro allows unnamed types to appear only once in a union, but named types may appear multiple times and
          // thus need to be disambiguated via their full-name (including aliases).
          if (potentialReaderSchema.getType().equals(optionSchema.getType()) &&
              (!schemaAssistant.isNamedType(potentialReaderSchema) ||
                  potentialReaderSchema.getFullName().equals(optionSchema.getFullName()) ||
                  potentialReaderSchema.getAliases().contains(optionSchema.getFullName()))) {
            readerOptionSchema = potentialReaderSchema;
            readerOptionUnionBranchIndex = j;
            break;
          }
        }
        if (null == readerOptionSchema) {
          // This is the same exception that vanilla Avro would throw in this circumstance
          caseBody._throw(JExpr._new(codeModel.ref(AvroTypeException.class)).arg(
              JExpr.lit("Found " + optionSchema + ", expecting " + readerUnionSchema.getTypes().toString())));
          continue;
        }
        Symbol.Alternative alternative = null;
        if (action.getSymbol() instanceof Symbol.Alternative) {
          alternative = (Symbol.Alternative) action.getSymbol();
        } else if (action.getSymbol().production != null) {
          for (Symbol symbol : action.getSymbol().production) {
            if (symbol instanceof Symbol.Alternative) {
              alternative = (Symbol.Alternative) symbol;
              break;
            }
          }
        }

        if (alternative == null) {
          throw new FastDeserializerGeneratorException("Unable to determine action for field: " + name);
        }

        Symbol.UnionAdjustAction unionAdjustAction = (Symbol.UnionAdjustAction) alternative.symbols[i].production[0];
        unionAction =
            FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(), unionAdjustAction.symToParse);
      } else {
        unionAction = FieldAction.fromValues(optionSchema.getType(), false, EMPTY_SYMBOL);
      }

      JVar optionSchemaVar = null;
      if (useGenericTypes && unionAction.getShouldRead()) {
        if (-1 == readerOptionUnionBranchIndex) {
          // TODO: Improve the flow of this code... it's messy needing to check for this here...
          throw new FastSerdeGeneratorException("Unexpected internal state. The readerOptionUnionBranchIndex should be defined if unionAction.getShouldRead() == true.");
        }
        JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(readerOptionUnionBranchIndex));
        optionSchemaVar = declareSchemaVar(optionSchema, name + "OptionSchema", optionSchemaExpression);
      }

      if (SchemaAssistant.isComplexType(optionSchema)) {
        String optionName = name + "Option";
        if (Schema.Type.UNION.equals(optionSchema.getType())) {
          throw new FastDeserializerGeneratorException("Union cannot be sub-type of union!");
        }
        processComplexType(optionSchemaVar, optionName, optionSchema, readerOptionSchema, caseBody, unionAction,
            putValueIntoParent, reuseSupplier);
      } else {
        processSimpleType(optionSchema, readerOptionSchema, caseBody, unionAction, putValueIntoParent, reuseSupplier);
      }
      caseBody._break();
    }
    switchBlock._default().body()._throw(JExpr._new(codeModel.ref(RuntimeException.class)).arg(JExpr.lit("Illegal union index for '" + name + "': ").plus(unionIndex)));
  }

  private void processArray(JVar arraySchemaVar, final String name, final Schema arraySchema,
      final Schema readerArraySchema, JBlock parentBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putArrayIntoParent, Supplier<JExpression> reuseSupplier) {

    if (action.getShouldRead()) {
      Symbol valuesActionSymbol = null;
      for (Symbol symbol : action.getSymbol().production) {
        if (Symbol.Kind.REPEATER.equals(symbol.kind) && "array-end".equals(
            getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
          valuesActionSymbol = symbol;
          break;
        }
      }

      if (valuesActionSymbol == null) {
        throw new FastDeserializerGeneratorException("Unable to determine action for array: " + name);
      }

      action =
          FieldAction.fromValues(arraySchema.getElementType().getType(), action.getShouldRead(), valuesActionSymbol);
    } else {
      action = FieldAction.fromValues(arraySchema.getElementType().getType(), false, EMPTY_SYMBOL);
    }

    final JVar arrayVar = action.getShouldRead() ? declareValueVar(name, readerArraySchema, parentBody, true, false, true) : null;
    /**
     * Special optimization for float array by leveraging {@link ByteBufferBackedPrimitiveFloatList}.
     *
     * TODO: Handle other primitive element types here.
     */
    if (action.getShouldRead() && arraySchema.getElementType().getType().equals(Schema.Type.FLOAT)) {
      JClass primitiveFloatList = codeModel.ref(ByteBufferBackedPrimitiveFloatList.class);
      JExpression readPrimitiveFloatArrayInvocation = primitiveFloatList.staticInvoke("readPrimitiveFloatArray").
          arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
      JExpression castedResult =
          JExpr.cast(codeModel.ref(PrimitiveFloatList.class), readPrimitiveFloatArrayInvocation);

      parentBody.assign(arrayVar, castedResult);
      putArrayIntoParent.accept(parentBody, arrayVar);
      return;
    }

    JClass arrayClass = schemaAssistant.classFromSchema(action.getShouldRead() ? readerArraySchema : arraySchema, false, false, true);
    JClass abstractErasedArrayClass = schemaAssistant.classFromSchema(action.getShouldRead() ? readerArraySchema : arraySchema, true, false, true).erasure();

    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"), JExpr.direct(DECODER + ".readArrayStart()"));

    final FieldAction finalAction = action;
    JInvocation newArrayExp = JExpr._new(arrayClass);
    if (useGenericTypes) {
      newArrayExp = newArrayExp.arg(JExpr.cast(codeModel.INT, chunkLen));
      if (!SchemaAssistant.isPrimitive(arraySchema.getElementType())) {
        /**
         * N.B.: The ColdPrimitiveXList implementations do not take the schema as a constructor param,
         * but the {@link org.apache.avro.generic.GenericData.Array} does.
         */
        newArrayExp = newArrayExp.arg(getSchemaExpr(arraySchema));
      }
    }
    JInvocation finalNewArrayExp = newArrayExp;


    final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, parentBody, "oldArray");
    if (finalAction.getShouldRead()) {
      /** N.B.: Need to use the erasure because instanceof does not support generic types */
      ifCodeGen(parentBody, finalReuseSupplier.get()._instanceof(abstractErasedArrayClass), then2 -> {
        then2.assign(arrayVar, JExpr.cast(abstractErasedArrayClass, finalReuseSupplier.get()));
        then2.invoke(arrayVar, "clear");
      }, else2 -> {
        else2.assign(arrayVar, finalNewArrayExp);
      });
    }

    JExpression chunkLengthGreaterThanZero = chunkLen.gt(JExpr.lit(0));

    JWhileLoop whileLoopToIterateOnBlocks = parentBody._while(chunkLengthGreaterThanZero);
    JForLoop forLoopToIterateOnElementsOfCurrentBlock = whileLoopToIterateOnBlocks.body()._for();
    JVar counter = forLoopToIterateOnElementsOfCurrentBlock.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoopToIterateOnElementsOfCurrentBlock.test(counter.lt(chunkLen));
    forLoopToIterateOnElementsOfCurrentBlock.update(counter.incr());
    JBlock forBody = forLoopToIterateOnElementsOfCurrentBlock.body();

    JVar elementSchemaVar = null;
    BiConsumer<JBlock, JExpression> putValueInArray = null;
    if (finalAction.getShouldRead()) {
      String addMethod = SchemaAssistant.isPrimitive(arraySchema.getElementType())
          ? "addPrimitive"
          : "add";
      putValueInArray = (block, expression) -> block.invoke(arrayVar, addMethod).arg(expression);
      if (useGenericTypes) {
        elementSchemaVar = declareSchemaVar(arraySchema.getElementType(), name + "ArrayElemSchema",
            arraySchemaVar.invoke("getElementType"));
      }
    }

    Supplier<JExpression> elementReuseSupplier = null;
    if (SchemaAssistant.isCapableOfReuse(arraySchema.getElementType())) {
      // Define element reuse variable here (but only for mutable types)
      JVar elementReuseVar = forBody.decl(codeModel.ref(Object.class), getUniqueName(name + "ArrayElementReuseVar"), JExpr._null());
      ifCodeGen(forBody, finalReuseSupplier.get()._instanceof(codeModel.ref(GenericArray.class)), then2 -> {
        then2.assign(elementReuseVar, JExpr.invoke(JExpr.cast(codeModel.ref(GenericArray.class), finalReuseSupplier.get()), "peek"));
      });
      elementReuseSupplier = () -> elementReuseVar;
    }

    Schema readerArrayElementSchema = null;
    if (finalAction.getShouldRead()) {
      readerArrayElementSchema = readerArraySchema.getElementType();
    }

    if (SchemaAssistant.isComplexType(arraySchema.getElementType())) {
      String elemName = name + "Elem";

      processComplexType(elementSchemaVar, elemName, arraySchema.getElementType(), readerArrayElementSchema, forBody,
          finalAction, putValueInArray, elementReuseSupplier);
    } else {
      processSimpleType(arraySchema.getElementType(), readerArrayElementSchema, forBody, finalAction, putValueInArray, elementReuseSupplier);
    }
    whileLoopToIterateOnBlocks.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));

    if (action.getShouldRead()) {
      putArrayIntoParent.accept(parentBody, arrayVar);
    }
  }

  /**
   * Return a JExpression, which will read a string from decoder and construct a stringable object.
   * @param stringbleClass
   * @return
   */
  private JExpression readStringableExpression(JClass stringbleClass) {
    JExpression stringableArgExpr;
    if (Utils.isAvro14()) {
      /**
       * {@link BinaryDecoder#readString()} is not available in avro-1.4.
       */
      stringableArgExpr = JExpr.direct(DECODER + ".readString(null).toString()");
    } else {
      // More GC-efficient
      stringableArgExpr = JExpr.direct(DECODER + ".readString()");
    }
    return JExpr._new(stringbleClass).arg(stringableArgExpr);
  }

  private void processMap(JVar mapSchemaVar, final String name, final Schema mapSchema, final Schema readerMapSchema,
      JBlock parentBody, FieldAction action, BiConsumer<JBlock, JExpression> putMapIntoParent,
      Supplier<JExpression> reuseSupplier) {

    /**
     * Determine the action symbol for Map value. {@link ResolvingGrammarGenerator} generates
     * resolving grammar symbols with reversed order of production sequence. If this symbol is
     * a terminal, its production list will be <code>null</code>. Otherwise the production list
     * holds the the sequence of the symbols that forms this symbol.
     *
     * The {@link FastDeserializerGenerator.generateDeserializer} tries to proceed as a depth-first,
     * left-to-right traversal of the schema. So for a nested Map, we need to iterate production list
     * in reverse order to get the correct "map-end" symbol of internal Maps.
     */
    if (action.getShouldRead()) {
      Symbol valuesActionSymbol = null;
      for (int i = action.getSymbol().production.length - 1; i >= 0; --i) {
        Symbol symbol = action.getSymbol().production[i];
        if (Symbol.Kind.REPEATER.equals(symbol.kind) && "map-end".equals(
            getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
          valuesActionSymbol = symbol;
          break;
        }
      }

      if (valuesActionSymbol == null) {
        throw new FastDeserializerGeneratorException("unable to determine action for map: " + name);
      }

      action = FieldAction.fromValues(mapSchema.getValueType().getType(), action.getShouldRead(), valuesActionSymbol);
    } else {
      action = FieldAction.fromValues(mapSchema.getValueType().getType(), false, EMPTY_SYMBOL);
    }

    final JVar mapVar = action.getShouldRead() ? declareValueVar(name, readerMapSchema, parentBody) : null;
    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"), JExpr.direct(DECODER + ".readMapStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlockForChunkLenCheck = conditional._then();

    if (action.getShouldRead()) {
      JVar reuse = declareValueVar(name + "Reuse", readerMapSchema, ifBlockForChunkLenCheck);

      // Check whether the reuse is a Map or not
      final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, ifBlockForChunkLenCheck, "oldMap");
      ifCodeGen(ifBlockForChunkLenCheck,
          finalReuseSupplier.get()._instanceof(codeModel.ref(Map.class)),
          thenBlock -> thenBlock.assign(reuse, JExpr.cast(codeModel.ref(Map.class), finalReuseSupplier.get())));

      // Check whether the reuse is null or not
      ifCodeGen(ifBlockForChunkLenCheck,
          reuse.ne(JExpr.direct("null")),
          thenBlock -> {
            thenBlock.invoke(reuse, "clear");
            thenBlock.assign(mapVar, reuse);
          },
          elseBlock -> elseBlock.assign(mapVar, JExpr._new(schemaAssistant.classFromSchema(readerMapSchema, false)))
      );

      JBlock elseBlock = conditional._else();
      elseBlock.assign(mapVar, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
    }

    JDoLoop doLoop = ifBlockForChunkLenCheck._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JClass keyClass = schemaAssistant.findStringClass(action.getShouldRead() ? readerMapSchema : mapSchema);
    JExpression keyValueExpression;
    if (SchemaAssistant.hasStringableKey(mapSchema)) {
      keyValueExpression = readStringableExpression(keyClass);
    } else {
      keyValueExpression = codeModel.ref(String.class).equals(keyClass) ?
              JExpr.direct(DECODER + ".readString()")
              : JExpr.direct(DECODER + ".readString(null)");
    }

    JVar key = forBody.decl(keyClass, getUniqueName("key"), keyValueExpression);
    JVar mapValueSchemaVar = null;
    if (action.getShouldRead() && useGenericTypes) {
      mapValueSchemaVar =
          declareSchemaVar(mapSchema.getValueType(), name + "MapValueSchema", mapSchemaVar.invoke("getValueType"));
    }

    BiConsumer<JBlock, JExpression> putValueInMap = null;
    Schema readerMapValueSchema = null;
    if (action.getShouldRead()) {
      putValueInMap = (block, expression) -> block.invoke(mapVar, "put").arg(key).arg(expression);
      readerMapValueSchema = readerMapSchema.getValueType();
    }

    if (SchemaAssistant.isComplexType(mapSchema.getValueType())) {
      String valueName = name + "Value";
      processComplexType(mapValueSchemaVar, valueName, mapSchema.getValueType(), readerMapValueSchema, forBody, action,
          putValueInMap, EMPTY_SUPPLIER);
    } else {
      processSimpleType(mapSchema.getValueType(), readerMapValueSchema, forBody, action, putValueInMap, EMPTY_SUPPLIER);
    }
    doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));
    if (action.getShouldRead()) {
      putMapIntoParent.accept(parentBody, mapVar);
    }
  }

  private void processFixed(final Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putFixedIntoParent, Supplier<JExpression> reuseSupplier) {
    if (action.getShouldRead()) {
      JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getUniqueName(schema.getName()), null);
      if (reuseSupplier.get().equals(JExpr._null())) {
        body.assign(fixedBuffer, JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));
      } else {
        /**
         * Here will check whether the length of the reused fixed is same as the one to be deserialized or not.
         * If not, here will initialize a new byte array to store it.
         */
        JVar oldValue = body.decl(codeModel.ref(Object.class), getUniqueName("oldFixed"), reuseSupplier.get());
        ifCodeGen(body,
            oldValue._instanceof(codeModel.ref(GenericFixed.class)).
                cand(JExpr.invoke(JExpr.cast(codeModel.ref(GenericFixed.class), oldValue), "bytes")
                    .ref("length")
                    .eq(JExpr.direct("" + schema.getFixedSize()))),
            thenBlock -> thenBlock.assign(fixedBuffer,
                JExpr.invoke(JExpr.cast(codeModel.ref(GenericFixed.class), oldValue), "bytes")),
            elseBlock -> elseBlock.assign(fixedBuffer, JExpr.direct(" new byte[" + schema.getFixedSize() + "]"))
        );
      }
      body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");

      JClass fixedClass = schemaAssistant.classFromSchema(schema);
      if (useGenericTypes) {
        JInvocation newFixedExpr;
        if (Utils.isAvro14()) {
          newFixedExpr = JExpr._new(fixedClass).arg(fixedBuffer);
        } else {
          newFixedExpr = JExpr._new(fixedClass).arg(getSchemaExpr(schema)).arg(fixedBuffer);
        }
        putFixedIntoParent.accept(body, newFixedExpr);
      } else {
        // fixed implementation in avro-1.4
        // The specific fixed type only has a constructor with empty param
        JVar fixed = body.decl(fixedClass, getUniqueName(schema.getName()));
        JInvocation newFixedExpr = JExpr._new(fixedClass);
        body.assign(fixed, newFixedExpr);
        body.directStatement(fixed.name() + ".bytes(" + fixedBuffer.name() + ");");
        putFixedIntoParent.accept(body, fixed);
      }
    } else {
      body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
    }
  }

  private void processEnum(final Schema schema, final JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putEnumIntoParent) {

    if (action.getShouldRead()) {

      Symbol.EnumAdjustAction enumAdjustAction = null;
      if (action.getSymbol() instanceof Symbol.EnumAdjustAction) {
        enumAdjustAction = (Symbol.EnumAdjustAction) action.getSymbol();
      } else {
        for (Symbol symbol : action.getSymbol().production) {
          if (symbol instanceof Symbol.EnumAdjustAction) {
            enumAdjustAction = (Symbol.EnumAdjustAction) symbol;
          }
        }
      }

      boolean enumOrderCorrect = true;
      for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
        Object adjustment = enumAdjustAction.adjustments[i];
        if (adjustment instanceof String) {
          throw new FastDeserializerGeneratorException(
              schema.getName() + " enum label impossible to deserialize: " + adjustment.toString());
        } else if (!adjustment.equals(i)) {
          enumOrderCorrect = false;
        }
      }

      JExpression newEnum;
      JExpression enumValueExpr = JExpr.direct(DECODER + ".readEnum()");

      if (enumOrderCorrect) {
        newEnum = schemaAssistant.getEnumValueByIndex(schema, enumValueExpr, getSchemaExpr(schema));
      } else {
        JVar enumIndex = body.decl(codeModel.INT, getUniqueName("enumIndex"), enumValueExpr);
        JClass enumClass = schemaAssistant.classFromSchema(schema);
        newEnum = body.decl(enumClass, getUniqueName("enumValue"), JExpr._null());

        JConditional ifBlock = null;
        for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
          JExpression ithVal =
              schemaAssistant.getEnumValueByIndex(schema, JExpr.lit((Integer) enumAdjustAction.adjustments[i]),
                  getSchemaExpr(schema));
          JExpression condition = enumIndex.eq(JExpr.lit(i));
          ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
          ifBlock._then().assign((JVar) newEnum, ithVal);
        }
      }
      putEnumIntoParent.accept(body, newEnum);
    } else {
      body.directStatement(DECODER + ".readEnum();");
    }
  }

  private void processBytes(JBlock body, FieldAction action, BiConsumer<JBlock, JExpression> putValueIntoParent,
      Supplier<JExpression> reuseSupplier) {
    if (action.getShouldRead()) {
      if (reuseSupplier.get().equals(JExpr._null())) {
        putValueIntoParent.accept(body, JExpr.invoke(JExpr.direct(DECODER), "readBytes").arg(JExpr.direct("null")));
      } else {
        final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, body, "oldBytes");
        ifCodeGen(body,
            finalReuseSupplier.get()._instanceof(codeModel.ref("java.nio.ByteBuffer")),
            thenBlock -> putValueIntoParent.accept(thenBlock, JExpr.invoke(JExpr.direct(DECODER), "readBytes")
                .arg(JExpr.cast(codeModel.ref(ByteBuffer.class), finalReuseSupplier.get()))),
            elseBlock -> putValueIntoParent.accept(elseBlock,
                JExpr.invoke(JExpr.direct(DECODER), "readBytes").arg(JExpr.direct("null")))
        );
      }
    } else {
      body.directStatement(DECODER + ".skipBytes()");
    }
  }

  private void processString(Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent, Supplier<JExpression> reuseSupplier) {
    if (action.getShouldRead()) {
      JClass stringClass = schemaAssistant.findStringClass(schema);
      if (stringClass.equals(codeModel.ref(Utf8.class))) {
        if (reuseSupplier.equals(EMPTY_SUPPLIER)) {
          putValueIntoParent.accept(body, JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr._null()));
        } else {
          final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, body, "oldString");
          ifCodeGen(body, finalReuseSupplier.get()._instanceof(codeModel.ref(Utf8.class)),
              thenBlock -> putValueIntoParent.accept(thenBlock, JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr.cast(codeModel.ref(Utf8.class), finalReuseSupplier.get()))),
              elseBlock -> putValueIntoParent.accept(elseBlock,
                  JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr._null())));
        }
      } else if (stringClass.equals(codeModel.ref(String.class))) {
        putValueIntoParent.accept(body, JExpr.invoke(JExpr.direct(DECODER), "readString"));
      } else {
        putValueIntoParent.accept(body, readStringableExpression(stringClass));
      }
    } else {
      body.directStatement(DECODER + ".skipString();");
    }
  }

  private void processPrimitive(final Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent, Supplier<JExpression> reuseSupplier) {

    String readFunction;
    switch (schema.getType()) {
      case STRING:
        processString(schema, body, action, putValueIntoParent, reuseSupplier);
        return;
      case BYTES:
        processBytes(body, action, putValueIntoParent, reuseSupplier);
        return;
      case INT:
        readFunction = "readInt()";
        break;
      case LONG:
        readFunction = "readLong()";
        break;
      case FLOAT:
        readFunction = "readFloat()";
        break;
      case DOUBLE:
        readFunction = "readDouble()";
        break;
      case BOOLEAN:
        readFunction = "readBoolean()";
        break;
      default:
        throw new FastDeserializerGeneratorException("Unsupported primitive schema of type: " + schema.getType());
    }

    JExpression primitiveValueExpression = JExpr.direct("decoder." + readFunction);
    if (action.getShouldRead()) {
      putValueIntoParent.accept(body, primitiveValueExpression);
    } else {
      body.directStatement(DECODER + "." + readFunction + ";");
    }
  }

  private JVar declareSchemaVariableForRecordField(final String name, final Schema schema, JVar schemaVar) {
    return declareSchemaVar(schema, name + "Field", schemaVar.invoke("getField").arg(name).invoke("schema"));
  }

  private JVar declareSchemaVar(Schema valueSchema, String variableName, JInvocation getValueType) {
    if (!useGenericTypes) {
      return null;
    }
    if (SchemaAssistant.isComplexType(valueSchema) || Schema.Type.ENUM.equals(valueSchema.getType())) {
      long schemaId = Utils.getSchemaFingerprint(valueSchema);
      if (schemaVarMap.get(schemaId) != null) {
        return schemaVarMap.get(schemaId);
      } else {
        JVar schemaVar = generatedClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class,
            getUniqueName(StringUtils.uncapitalize(variableName)));
        constructor.body().assign(JExpr.refthis(schemaVar.name()), getValueType);

        registerSchema(valueSchema, schemaId, schemaVar);
        return schemaVar;
      }
    } else {
      return null;
    }
  }

  private void registerSchema(final Schema writerSchema, JVar schemaVar) {
    registerSchema(writerSchema, Utils.getSchemaFingerprint(writerSchema), schemaVar);
  }

  private void registerSchema(final Schema writerSchema, long schemaId, JVar schemaVar) {
    if ((Schema.Type.RECORD.equals(writerSchema.getType()) || Schema.Type.ENUM.equals(writerSchema.getType())
        || Schema.Type.ARRAY.equals(writerSchema.getType())) && schemaNotRegistered(writerSchema)) {
      schemaMap.put(schemaId, writerSchema);
      schemaVarMap.put(schemaId, schemaVar);
    }
  }

  private boolean schemaNotRegistered(final Schema schema) {
    return !schemaMap.containsKey(Utils.getSchemaFingerprint(schema));
  }

  private boolean methodAlreadyDefined(final Schema schema, boolean read) {
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + schema.getType());
    }

    return (read ? deserializeMethodMap : skipMethodMap).containsKey(schema.getFullName());
  }

  private JMethod getMethod(final Schema schema, boolean read) {
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + schema.getType());
    }
    if (!methodAlreadyDefined(schema, read)) {
      throw new FastDeserializerGeneratorException("No method for schema: " + schema.getFullName());
    }
    return (read ? deserializeMethodMap : skipMethodMap).get(schema.getFullName());
  }

  private JMethod createMethod(final Schema schema, boolean read) {
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + schema.getType());
    }
    if (methodAlreadyDefined(schema, read)) {
      throw new FastDeserializerGeneratorException("Method already exists for: " + schema.getFullName());
    }

    JClass schemaClass = schemaAssistant.classFromSchema(schema);
    JMethod method = generatedClass.method(JMod.PUBLIC, read ? schemaClass : codeModel.VOID,
        getUniqueName("deserialize" + schema.getName()));

    method._throws(IOException.class);
    method.param(Object.class, VAR_NAME_FOR_REUSE);
    method.param(Decoder.class, DECODER);

    (read ? deserializeMethodMap : skipMethodMap).put(schema.getFullName(), method);

    return method;
  }

  private JExpression getSchemaExpr(Schema schema) {
    Long index = Utils.getSchemaFingerprint(schema);
    return (useGenericTypes && schemaVarMap.containsKey(index)) ? schemaVarMap.get(index) : JExpr._null();
  }

  private Supplier<JExpression> potentiallyCacheInvocation(Supplier<JExpression> jExpressionSupplier, JBlock body, String variableNamePrefix) {
    if (jExpressionSupplier.get() instanceof JInvocation) {
      JVar oldValue = body.decl(codeModel.ref(Object.class), getUniqueName(variableNamePrefix), jExpressionSupplier.get());
      return () -> oldValue;
    }
    return jExpressionSupplier;
  }
}
