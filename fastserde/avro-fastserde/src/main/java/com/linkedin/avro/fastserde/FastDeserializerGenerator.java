package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.backport.ResolvingGrammarGenerator;
import com.linkedin.avro.fastserde.backport.Symbol;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
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
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import com.sun.codemodel.JWhileLoop;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastDeserializerGenerator.class);
  private static final String DECODER = "decoder";
  private static final String VAR_NAME_FOR_REUSE = "reuse";
  private static int FIELDS_PER_POPULATION_METHOD = 100;

  // 65535 is the actual limit, 65K added for safety
  static int MAX_LENGTH_OF_STRING_LITERAL = 65000;

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
        registerSchema(reader, readerSchemaVar);
      }

      JClass readerSchemaClass = schemaAssistant.classFromSchema(reader);
      /**
       * Writer schema could be using a different namespace from the reader schema, so we should always
       * use the reader schema class for generic type.
       */
      generatedClass._implements(codeModel.ref(FastDeserializer.class).narrow(readerSchemaClass));
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
        case UNION:
          processUnion(readerSchemaVar, "union", aliasedWriterSchema, reader, topLevelDeserializeBlock, fieldAction,
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
    } catch (FastDeserializerGeneratorException e) {
      throw e;
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
        processEnum(readerSchema, methodBody, action, putExpressionIntoParent);
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

    Schema effectiveRecordReaderSchema = recordReaderSchema;
    if (recordAction.getShouldRead()) {

      // if reader schema is a union type then the compatible union type must be selected
      // as effectiveRecordReaderSchema and recordAction needs to be adjusted.
      if (Schema.Type.UNION.equals(recordReaderSchema.getType())) {
        effectiveRecordReaderSchema = schemaAssistant.compatibleUnionSchema(recordWriterSchema, recordReaderSchema);
        recordSchemaVar = declareSchemaVar(effectiveRecordReaderSchema, recordName + "RecordSchema",
                recordSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(
                        schemaAssistant.compatibleUnionSchemaIndex(recordWriterSchema, recordReaderSchema)
                )));

        Symbol symbol = null;
        ListIterator<Symbol> symbolIterator = recordAction.getSymbolIterator() != null ? recordAction.getSymbolIterator()
                : Arrays.asList(reverseSymbolArray(recordAction.getSymbol().production)).listIterator();
        while (symbolIterator.hasNext()) {
          symbol = symbolIterator.next();

          if (symbol instanceof Symbol.UnionAdjustAction) {
            break;
          }
        }
        if (symbol == null) {
          throw new FastDeserializerGeneratorException("Symbol.UnionAdjustAction is expected but was not found");
        }
        recordAction = FieldAction.fromValues(recordAction.getType(), recordAction.getShouldRead(),
                ((Symbol.UnionAdjustAction) symbol).symToParse);
      }
    }
    ListIterator<Symbol> actionIterator = actionIterator(recordAction);

    if (methodAlreadyDefined(recordWriterSchema, effectiveRecordReaderSchema, recordAction.getShouldRead())) {
      JMethod method = getMethod(recordWriterSchema, effectiveRecordReaderSchema, recordAction.getShouldRead());
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
      for (Schema.Field readerField : effectiveRecordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
        }
      }
      return;
    }

    JMethod method = createMethod(recordWriterSchema, effectiveRecordReaderSchema, recordAction.getShouldRead());

    Set<Class<? extends Exception>> exceptionsOnHigherLevel = schemaAssistant.getExceptionsFromStringable();
    schemaAssistant.resetExceptionsFromStringable();

    if (recordAction.getShouldRead()) {
      putRecordIntoParent.accept(parentBody, JExpr.invoke(method).arg(reuseSupplier.get()).arg(JExpr.direct(DECODER)));
    } else {
      parentBody.invoke(method).arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
    }

    JBlock methodBody = method.body();

    final JVar result;
    JClass recordClass = null;
    if (recordAction.getShouldRead()) {
      recordClass = schemaAssistant.classFromSchema(effectiveRecordReaderSchema);
      result = methodBody.decl(recordClass, recordName);

      JExpression reuseVar = JExpr.direct(VAR_NAME_FOR_REUSE);
      JClass indexedRecordClass = codeModel.ref(IndexedRecord.class);
      JInvocation newRecord = JExpr._new(schemaAssistant.classFromSchema(effectiveRecordReaderSchema, false));
      if (useGenericTypes) {
        JExpression recordSchema = schemaVarMap.get(Utils.getSchemaFingerprint(effectiveRecordReaderSchema));
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
        JClass finalRecordClass = recordClass;
        ifCodeGen(methodBody,
            reuseVar.ne(JExpr._null()),
            thenBlock -> thenBlock.assign(result, JExpr.cast(finalRecordClass, reuseVar)),
            elseBlock -> elseBlock.assign(result, finalNewRecordInvocation)
        );
      }
    } else {
      result = null;
    }

    int fieldCount = 0;
    JBlock popMethodBody = methodBody;
    JMethod popMethod = null;
    for (Schema.Field field : recordWriterSchema.getFields()) {
      // We roll the population method for very large records, the initial fields are kept in the outer method as original to maintain performance for smaller records
      fieldCount++;
      if (fieldCount % FIELDS_PER_POPULATION_METHOD == 0) {
        popMethod = generatedClass.method(JMod.PRIVATE, codeModel.VOID,
                getUniqueName("populate_" + recordName));

        popMethod._throws(IOException.class);
        if (recordAction.getShouldRead()) {
          popMethod.param(recordClass, recordName);
        }
        popMethod.param(Decoder.class, DECODER);
        popMethodBody = popMethod.body();

        JInvocation invocation = methodBody.invoke(popMethod);
        if (recordAction.getShouldRead()) {
          invocation.arg(JExpr.direct(recordName));
        }
        invocation.arg(JExpr.direct(DECODER));
      }
      FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
      if (action.getSymbol() == END_SYMBOL) {
        break;
      }

      Schema readerFieldSchema = null;
      JVar fieldSchemaVar = null;
      BiConsumer<JBlock, JExpression> putExpressionInRecord = null;
      Supplier<JExpression> fieldReuseSupplier = EMPTY_SUPPLIER;
      if (action.getShouldRead()) {
        Schema.Field readerField = effectiveRecordReaderSchema.getField(field.name());
        readerFieldSchema = readerField.schema();
        final int readerFieldPos = readerField.pos();
        putExpressionInRecord =
            (block, expression) -> block.invoke(result, "put").arg(JExpr.lit(readerFieldPos)).arg(expression);
        if (useGenericTypes) {
          fieldSchemaVar = declareSchemaVar(readerField.schema(), readerField.name(),
              recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
        }
        fieldReuseSupplier = () -> result.invoke("get").arg(JExpr.lit(readerFieldPos));
      }
      if (SchemaAssistant.isComplexType(field.schema())) {
        processComplexType(fieldSchemaVar, field.name(), field.schema(), readerFieldSchema, popMethodBody, action,
            putExpressionInRecord, fieldReuseSupplier);
      } else {
        processSimpleType(field.schema(), readerFieldSchema, popMethodBody, action, putExpressionInRecord, fieldReuseSupplier);
      }

      if (popMethod != null) {
        for(Class<? extends Exception> e: schemaAssistant.getExceptionsFromStringable()) {
          popMethod._throws(e);
        }
      }
    }

    // Handle default values
    if (recordAction.getShouldRead()) {
      Set<String> fieldNamesSet =
          recordWriterSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
      for (Schema.Field readerField : effectiveRecordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
          JVar schemaVar = null;
          if (useGenericTypes) {
            schemaVar = declareSchemaVariableForRecordField(readerField.name(), readerField.schema(), recordSchemaVar);
          }
          JExpression value = parseDefaultValue(
              readerField.schema(),
              AvroCompatibilityHelper.getGenericDefaultValue(readerField),
              methodBody,
              schemaVar,
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

  private JExpression parseDefaultValue(Schema schema, Object defaultValue, JBlock body, JVar schemaVar,
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

          GenericRecord defaultValueRecord = (GenericRecord) defaultValue;

          for (Schema.Field subField : schema.getFields()) {

            JVar fieldSchemaVar = null;
            if (useGenericTypes) {
              fieldSchemaVar = declareSchemaVariableForRecordField(subField.name(), subField.schema(), schemaVar);
            }
            JExpression fieldValue =
                parseDefaultValue(subField.schema(), defaultValueRecord.get(subField.name()), body, fieldSchemaVar, subField.name());
            body.invoke(valueVar, "put").arg(JExpr.lit(subField.pos())).arg(fieldValue);

          }
          break;
        case ARRAY:
          JVar elementSchemaVar = null;
          List<Object> defaultValueList = (List<Object>) defaultValue;
          if (useGenericTypes) {
            valueInitializationExpr =
                valueInitializationExpr.arg(JExpr.lit(defaultValueList.size())).arg(getSchemaExpr(schema));
            elementSchemaVar =
                declareSchemaVar(schema.getElementType(), "defaultElementSchema", schemaVar.invoke("getElementType"));
          }

          valueVar = body.decl(defaultValueClass, getUniqueName("defaultArray"), valueInitializationExpr);

          for (Object arrayElementValue : defaultValueList) {
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

          Map<CharSequence, Object> defaultValueMap = (Map<CharSequence, Object>) defaultValue;

          for (Map.Entry<CharSequence, Object> mapEntry : defaultValueMap.entrySet()) {
            JExpression mapKeyExpr;
            if (SchemaAssistant.hasStringableKey(schema)) {
              mapKeyExpr = JExpr._new(schemaAssistant.findStringClass(schema)).arg(mapEntry.getKey().toString());
            } else {
              mapKeyExpr = JExpr._new(codeModel.ref(Utf8.class)).arg(mapEntry.getKey().toString());
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
          GenericData.EnumSymbol defaultValueEnum = (GenericData.EnumSymbol) defaultValue;
          return schemaAssistant.getEnumValueByName(schema, JExpr.lit(defaultValueEnum.toString()),
              getSchemaExpr(schema));
        case FIXED:
          GenericData.Fixed defaultValueFixedLengthBytes = (GenericData.Fixed) defaultValue;
          JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
          for (byte b : defaultValueFixedLengthBytes.bytes()) {
            fixedBytesArray.add(JExpr.lit(b));
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
          ByteBuffer defaultValueVariableLengthBytes = (ByteBuffer) defaultValue;
          JArray bytesArray = JExpr.newArray(codeModel.BYTE);
          for (byte b : defaultValueVariableLengthBytes.array()) {
            bytesArray.add(JExpr.lit(b));
          }
          return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
        case STRING:
          CharSequence defaultValueString = (CharSequence) defaultValue;
          return schemaAssistant.getStringableValue(schema, JExpr.lit(defaultValueString.toString()));
        case INT:
          Integer defaultValueInt = (Integer) defaultValue;
          return JExpr.lit(defaultValueInt);
        case LONG:
          Long defaultValueLong = (Long) defaultValue;
          return JExpr.lit(defaultValueLong);
        case FLOAT:
          Float defaultValueFloat = (Float) defaultValue;
          return JExpr.lit(defaultValueFloat);
        case DOUBLE:
          Double defaultValueDouble = (Double) defaultValue;
          return JExpr.lit(defaultValueDouble);
        case BOOLEAN:
          Boolean defaultValueBoolean = (Boolean) defaultValue;
          return JExpr.lit(defaultValueBoolean);
        case NULL:
        default:
          throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
      }
    }
  }

  private void processUnion(JVar unionSchemaVar, final String name, final Schema unionSchema,
      final Schema unionReaderSchema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent, Supplier<JExpression> reuseSupplier) {
    JVar unionIndex = body.decl(codeModel.INT, getUniqueName("unionIndex"), JExpr.direct(DECODER + ".readIndex()"));
    JConditional ifBlock = null;

    // Check if unionReaderSchema is really a union, if not then only the compatible writer union type can be deserialized
    final boolean readerSchemaNotAUnion = unionReaderSchema != null && !Schema.Type.UNION.equals(unionReaderSchema.getType());
    final int compatibleWriterSchema = readerSchemaNotAUnion ? schemaAssistant.compatibleUnionSchemaIndex(unionReaderSchema, unionSchema) : -1;

    for (int i = 0; i < unionSchema.getTypes().size(); i++) {
      Schema optionSchema = unionSchema.getTypes().get(i);
      Schema readerOptionSchema = null;
      int readerOptionUnionBranchIndex = -1;
      FieldAction unionAction;
      JExpression condition = unionIndex.eq(JExpr.lit(i));

      ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
      final JBlock thenBlock = ifBlock._then();

      if (!readerSchemaNotAUnion && Schema.Type.NULL.equals(optionSchema.getType())) {
        thenBlock.directStatement(DECODER + ".readNull();");
        if (action.getShouldRead()) {
          putValueIntoParent.accept(thenBlock, JExpr._null());
        }
        continue;
      }

      if (action.getShouldRead()) {
        // If unionReaderSchema is not a union then only compatible writer union schema type can be processed,
        // otherwise the readerOptionSchema will be set to null what should result in AvroTypeException as
        // in vanilla avro.
        if (readerSchemaNotAUnion) {
            readerOptionSchema = (i == compatibleWriterSchema) ? unionReaderSchema : null;
        } else {
          // The reader's union could be re-ordered, so we need to find the one that matches.
          // TODO: this code should support primitive type promotions
          for (int j = 0; j < unionReaderSchema.getTypes().size(); j++) {
            Schema potentialReaderSchema = unionReaderSchema.getTypes().get(j);
            // Avro allows unnamed types to appear only once in a union, but named types may appear multiple times and
            // thus need to be disambiguated via their full-name (including aliases).
            if (potentialReaderSchema.getType().equals(optionSchema.getType()) &&
                (!schemaAssistant.isNamedType(potentialReaderSchema) ||
                    AvroCompatibilityHelper.getSchemaFullName(potentialReaderSchema).equals(AvroCompatibilityHelper.getSchemaFullName(optionSchema)) ||
                    potentialReaderSchema.getAliases().contains(AvroCompatibilityHelper.getSchemaFullName(optionSchema)))) {
              readerOptionSchema = potentialReaderSchema;
              readerOptionUnionBranchIndex = j;
              break;
            }
          }
        }

        if (null == readerOptionSchema) {
          // This is the same exception that vanilla Avro would throw in this circumstance
          String fullExceptionString = "Found " + optionSchema + ", expecting " + (readerSchemaNotAUnion ? unionReaderSchema.toString()
              : unionReaderSchema.getTypes().toString());
          thenBlock._throw(JExpr._new(codeModel.ref(AvroTypeException.class)).arg(
              composeStringLiteral(codeModel, fullExceptionString)));
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

        if (readerSchemaNotAUnion) {
          unionAction =
                  FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(), alternative.symbols[compatibleWriterSchema]);
        } else {
          Symbol.UnionAdjustAction unionAdjustAction = (Symbol.UnionAdjustAction) alternative.symbols[i].production[0];
          unionAction =
                  FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(), unionAdjustAction.symToParse);
        }

      } else {
        unionAction = FieldAction.fromValues(optionSchema.getType(), false, EMPTY_SYMBOL);
      }

      JVar optionSchemaVar = null;
      if (useGenericTypes && unionAction.getShouldRead() && !readerSchemaNotAUnion) {
        if (-1 == readerOptionUnionBranchIndex) {
          // TODO: Improve the flow of this code... it's messy needing to check for this here...
          throw new FastSerdeGeneratorException("Unexpected internal state. The readerOptionUnionBranchIndex should be defined if unionAction.getShouldRead() == true.");
        }
        JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(readerOptionUnionBranchIndex));
        optionSchemaVar = declareSchemaVar(readerOptionSchema, name + "OptionSchema", optionSchemaExpression);
      } else if (readerSchemaNotAUnion) {
        optionSchemaVar = unionSchemaVar;
      }

      if (SchemaAssistant.isComplexType(optionSchema)) {
        String optionName = name + "Option";
        if (Schema.Type.UNION.equals(optionSchema.getType())) {
          throw new FastDeserializerGeneratorException("Union cannot be sub-type of union!");
        }
        processComplexType(optionSchemaVar, optionName, optionSchema, readerOptionSchema, thenBlock, unionAction,
            putValueIntoParent, reuseSupplier);
      } else {
        processSimpleType(optionSchema, readerOptionSchema, thenBlock, unionAction, putValueIntoParent, reuseSupplier);
      }
    }
    if (ifBlock != null) {
      ifBlock._else()._throw(JExpr._new(codeModel.ref(RuntimeException.class)).arg(JExpr.lit("Illegal union index for '" + name + "': ").plus(unionIndex)));
    }
  }

  private void processArray(JVar arraySchemaVar, final String name, final Schema arraySchema,
      final Schema arrayReaderSchema, JBlock parentBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putArrayIntoParent, Supplier<JExpression> reuseSupplier) {

    Schema effectiveArrayReaderSchema = arrayReaderSchema;
    if (action.getShouldRead()) {

      // if reader schema is a union then the compatible union array type must be selected
      // as effectiveArrayReaderSchema and action needs to be adjusted.
      if (Schema.Type.UNION.equals(arrayReaderSchema.getType())) {
        effectiveArrayReaderSchema = schemaAssistant.compatibleUnionSchema(arraySchema, arrayReaderSchema);
        arraySchemaVar = declareSchemaVar(effectiveArrayReaderSchema, name + "ArraySchema",
                arraySchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(
                        schemaAssistant.compatibleUnionSchemaIndex(arraySchema, arrayReaderSchema)
                )));

        Symbol symbol = action.getSymbol();
        if (!(symbol instanceof Symbol.UnionAdjustAction)) {
          for (Symbol aSymbol: symbol.production) {
            if (aSymbol instanceof Symbol.UnionAdjustAction) {
              symbol = aSymbol;
              break;
            }
          }
        }
        if (symbol == null) {
          throw new FastDeserializerGeneratorException("Symbol.UnionAdjustAction is expected but was not found");
        }
        action = FieldAction.fromValues(action.getType(), action.getShouldRead(),
                ((Symbol.UnionAdjustAction) symbol).symToParse);
      }

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

    final JVar arrayVar = action.getShouldRead() ? declareValueVar(name, effectiveArrayReaderSchema, parentBody, true, false, true) : null;
    /**
     * Special optimization for float array by leveraging {@link BufferBackedPrimitiveFloatList}.
     *
     * TODO: Handle other primitive element types here.
     */
    if (action.getShouldRead() && arraySchema.getElementType().getType().equals(Schema.Type.FLOAT)) {
      JClass primitiveFloatList = codeModel.ref(BufferBackedPrimitiveFloatList.class);
      JExpression readPrimitiveFloatArrayInvocation = primitiveFloatList.staticInvoke("readPrimitiveFloatArray").
          arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
      JExpression castedResult =
          JExpr.cast(codeModel.ref(PrimitiveFloatList.class), readPrimitiveFloatArrayInvocation);

      parentBody.assign(arrayVar, castedResult);
      putArrayIntoParent.accept(parentBody, arrayVar);
      return;
    }

    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"), JExpr.direct(DECODER + ".readArrayStart()"));

    final FieldAction finalAction = action;

    final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, parentBody, "oldArray");
    if (finalAction.getShouldRead()) {

      JClass arrayClass = schemaAssistant.classFromSchema(effectiveArrayReaderSchema, false, false, true);
      JClass abstractErasedArrayClass = schemaAssistant.classFromSchema(effectiveArrayReaderSchema, true, false, true).erasure();

      JInvocation newArrayExp = JExpr._new(arrayClass).arg(JExpr.cast(codeModel.INT, chunkLen));
      if (useGenericTypes && !SchemaAssistant.isPrimitive(effectiveArrayReaderSchema.getElementType())) {
        /**
         * N.B.: The ColdPrimitiveXList implementations do not take the schema as a constructor param,
         * but the {@link org.apache.avro.generic.GenericData.Array} does.
         */
        newArrayExp = newArrayExp.arg(getSchemaExpr(effectiveArrayReaderSchema));
      }
      JInvocation finalNewArrayExp = newArrayExp;

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
      String addMethod = SchemaAssistant.isPrimitive(effectiveArrayReaderSchema.getElementType())
          ? "addPrimitive"
          : "add";
      putValueInArray = (block, expression) -> block.invoke(arrayVar, addMethod).arg(expression);
      if (useGenericTypes) {
        elementSchemaVar = declareSchemaVar(effectiveArrayReaderSchema.getElementType(), name + "ArrayElemSchema",
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
      readerArrayElementSchema = effectiveArrayReaderSchema.getElementType();
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
   *
   * @param stringbleClass
   * @return
   */
  private JExpression readStringableExpression(JClass stringbleClass) {
    JExpression stringableArgExpr;
    if (Utils.isAbleToSupportJavaStrings()) {
      // More GC-efficient
      stringableArgExpr = JExpr.direct(DECODER + ".readString()");
    } else {
      /**
       * {@link BinaryDecoder#readString()} is not available in Avro 1.4 and 1.5.
       */
      stringableArgExpr = JExpr.direct(DECODER + ".readString(null).toString()");
    }
    return JExpr._new(stringbleClass).arg(stringableArgExpr);
  }

  private void processMap(JVar mapSchemaVar, final String name, final Schema mapSchema, final Schema mapReaderSchema,
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
    Schema effectiveMapReaderSchema = mapReaderSchema;
    if (action.getShouldRead()) {
      // if reader schema is a union then the compatible union map type must be selected
      // as effectiveMapReaderSchema and action needs to be adjusted.
      if (Schema.Type.UNION.equals(mapReaderSchema.getType())) {
        effectiveMapReaderSchema = schemaAssistant.compatibleUnionSchema(mapSchema, mapReaderSchema);
        mapSchemaVar = declareSchemaVar(effectiveMapReaderSchema, name + "MapSchema",
                mapSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(
                        schemaAssistant.compatibleUnionSchemaIndex(mapSchema, mapReaderSchema)
                )));

        Symbol symbol = action.getSymbol();
        if (!(symbol instanceof Symbol.UnionAdjustAction)) {
          for (Symbol aSymbol: symbol.production) {
            if (aSymbol instanceof Symbol.UnionAdjustAction) {
              symbol = aSymbol;
              break;
            }
          }
        }
        if (symbol == null) {
          throw new FastDeserializerGeneratorException("Symbol.UnionAdjustAction is expected but was not found");
        }
        action = FieldAction.fromValues(action.getType(), action.getShouldRead(),
                ((Symbol.UnionAdjustAction) symbol).symToParse);
      }

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

    final JVar mapVar = action.getShouldRead() ? declareValueVar(name, effectiveMapReaderSchema, parentBody) : null;
    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"), JExpr.direct(DECODER + ".readMapStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlockForChunkLenCheck = conditional._then();

    if (action.getShouldRead()) {
      JVar reuse = declareValueVar(name + "Reuse", effectiveMapReaderSchema, ifBlockForChunkLenCheck);

      // Check whether the reuse is a Map or not
      final Supplier<JExpression> finalReuseSupplier = potentiallyCacheInvocation(reuseSupplier, ifBlockForChunkLenCheck, "oldMap");
      ifCodeGen(ifBlockForChunkLenCheck,
          finalReuseSupplier.get()._instanceof(codeModel.ref(Map.class)),
          thenBlock -> thenBlock.assign(reuse, JExpr.cast(codeModel.ref(Map.class), finalReuseSupplier.get())));

      // Check whether the reuse is null or not
      final Schema finalEffectiveMapReaderSchema = effectiveMapReaderSchema;
      ifCodeGen(ifBlockForChunkLenCheck,
          reuse.ne(JExpr.direct("null")),
          thenBlock -> {
            thenBlock.invoke(reuse, "clear");
            thenBlock.assign(mapVar, reuse);
          },
          // Pure integer arithmetic equivalent of (int) Math.ceil(expectedSize / 0.75).
          // The default load factor of HashMap is 0.75 and HashMap internally ensures size is always a power of two.
          elseBlock -> elseBlock.assign(mapVar, JExpr._new(schemaAssistant.classFromSchema(finalEffectiveMapReaderSchema, false))
              .arg(JExpr.cast(codeModel.INT, chunkLen.mul(JExpr.lit(4)).plus(JExpr.lit(2)).div(JExpr.lit(3)))))
      );

      JBlock elseBlock = conditional._else();
      elseBlock.assign(mapVar, JExpr._new(schemaAssistant.classFromSchema(effectiveMapReaderSchema, false))
          .arg(JExpr.lit(0)));
    }

    JDoLoop doLoop = ifBlockForChunkLenCheck._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JClass keyClass = schemaAssistant.findStringClass(action.getShouldRead() ? effectiveMapReaderSchema : mapSchema);
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
          declareSchemaVar(effectiveMapReaderSchema.getValueType(), name + "MapValueSchema", mapSchemaVar.invoke("getValueType"));
    }

    BiConsumer<JBlock, JExpression> putValueInMap = null;
    Schema readerMapValueSchema = null;
    if (action.getShouldRead()) {
      putValueInMap = (block, expression) -> block.invoke(mapVar, "put").arg(key).arg(expression);
      readerMapValueSchema = effectiveMapReaderSchema.getValueType();
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
      Set<Integer> unknownEnumIndexes = new HashSet<>();
      for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
        Object adjustment = enumAdjustAction.adjustments[i];
        if (!adjustment.equals(i)) {
          if (adjustment instanceof String) {
            unknownEnumIndexes.add(i);
          }
          enumOrderCorrect = false;
        }
      }

      JExpression newEnum;
      JExpression enumValueExpr = JExpr.direct(DECODER + ".readEnum()");

      if (enumOrderCorrect) {
        newEnum = schemaAssistant.getEnumValueByIndex(schema, enumValueExpr, getSchemaExpr(schema));
      } else {

        /**
         * Define a class variable to keep the mapping between the enum index from the writer schema and the corresponding
         * one in the reader schema, and there are some cases:
         * 1. If the enum index doesn't exist in this map, runtime will throw RuntimeException.
         * 2. If the enum index exists in the map, runtime will throw it if the mapping is an {@link AvroTypeException} because of unknown enum from reader schema.
         * 3. If there is a corresponding enum in the reader schema, runtime will use this one to lookup the exact Enum Symbol from the reader schema.
         */

        JVar tempEnumMappingVar = constructor.body().decl(codeModel.ref(HashMap.class),  getUniqueName("tempEnumMapping"),
            JExpr._new(codeModel.ref(HashMap.class)).arg(JExpr.lit(enumAdjustAction.adjustments.length)));
        /**
         * Populate the global enum mapping based on the enum adjustment.
         */
        for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
          JInvocation keyExpr = JExpr._new(codeModel.ref(Integer.class)).arg(JExpr.lit(i));
          JStatement mapUpdateStatement;
          if (unknownEnumIndexes.contains(i)) {
            JInvocation avroTypeExceptionExpr = JExpr._new(codeModel.ref(AvroTypeException.class))
                .arg(JExpr.lit(schema.getFullName() + ": " + enumAdjustAction.adjustments[i].toString()));
            mapUpdateStatement = tempEnumMappingVar.invoke("put").arg(keyExpr).arg(avroTypeExceptionExpr);
          } else {
            JInvocation valueExpr = JExpr._new(codeModel.ref(Integer.class)).arg(JExpr.lit((Integer)enumAdjustAction.adjustments[i]));
            mapUpdateStatement = tempEnumMappingVar.invoke("put").arg(keyExpr).arg(valueExpr);
          }
          constructor.body().add(mapUpdateStatement);
        }
        JVar enumMappingVar = generatedClass.field(JMod.PRIVATE | JMod.FINAL, Map.class, getUniqueName("enumMapping" + schema.getName()));
        constructor.body().assign(JExpr.refthis(enumMappingVar.name()), codeModel.ref(Collections.class).staticInvoke("unmodifiableMap").arg(tempEnumMappingVar));

        JVar enumIndex = body.decl(codeModel.INT, getUniqueName("enumIndex"), enumValueExpr);
        JClass enumClass = schemaAssistant.classFromSchema(schema);
        newEnum = body.decl(enumClass, getUniqueName("enumValue"), JExpr._null());

        JVar lookupResult = body.decl(codeModel._ref(Object.class), getUniqueName("enumIndexLookupResult"),
            enumMappingVar.invoke("get").arg(enumIndex));
        /**
         * Found the enum index mapping.
         */
        JConditional ifBlock = body._if(lookupResult._instanceof(codeModel.ref(Integer.class)));
        JExpression ithValResult =
            schemaAssistant.getEnumValueByIndex(schema, JExpr.cast(codeModel.ref(Integer.class), lookupResult), getSchemaExpr(schema));
        ifBlock._then().assign((JVar) newEnum, ithValResult);
        /**
         * Unknown enum in reader schema.
         */
        JConditional elseIfBlock = ifBlock._elseif(lookupResult._instanceof(codeModel.ref(AvroTypeException.class)));
        elseIfBlock._then()._throw(JExpr.cast(codeModel.ref(AvroTypeException.class), lookupResult));
        /**
         * Unknown enum in writer schema.
         */
        elseIfBlock._else()._throw(JExpr._new(codeModel.ref(RuntimeException.class))
            .arg(JExpr.lit("Illegal enum index for '" + schema.getFullName() + "': ").plus(enumIndex)));
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
      body.directStatement(DECODER + ".skipBytes();");
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
    /**
     * TODO: In theory, we should only need Record, Enum and Fixed here since only these types require
     * schema for the corresponding object initialization in Generic mode.
     */
    if (SchemaAssistant.isComplexType(valueSchema) || Schema.Type.ENUM.equals(valueSchema.getType())
        || Schema.Type.FIXED.equals(valueSchema.getType())) {
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
        // TODO: Do we need `ARRAY` type here?
        || Schema.Type.FIXED.equals(writerSchema.getType()) || Schema.Type.ARRAY.equals(writerSchema.getType()))
        && schemaNotRegistered(writerSchema)) {
      schemaMap.put(schemaId, writerSchema);
      schemaVarMap.put(schemaId, schemaVar);
    }
  }

  private boolean schemaNotRegistered(final Schema schema) {
    return !schemaMap.containsKey(Utils.getSchemaFingerprint(schema));
  }

  private boolean methodAlreadyDefined(final Schema writerSchema, final Schema readerSchema, boolean read) {
    if (!Schema.Type.RECORD.equals(writerSchema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + writerSchema.getType());
    }

    return (read ? deserializeMethodMap : skipMethodMap).containsKey(getEffectiveMethodName(writerSchema, readerSchema));
  }

  private String getEffectiveMethodName(Schema writerSchema, Schema readerSchema) {
    return AvroCompatibilityHelper.getSchemaFullName(writerSchema) + writerSchema.hashCode()
        + (readerSchema != null ? readerSchema.hashCode() : "");
  }

  private JMethod getMethod(final Schema writerSchema, final Schema readerSchema, boolean read) {
    if (!Schema.Type.RECORD.equals(writerSchema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + writerSchema.getType());
    }
    if (!methodAlreadyDefined(writerSchema, readerSchema, read)) {
      throw new FastDeserializerGeneratorException("No method for writerSchema: " + AvroCompatibilityHelper.getSchemaFullName(writerSchema));
    }
    return (read ? deserializeMethodMap : skipMethodMap).get(getEffectiveMethodName(writerSchema, readerSchema));
  }

  private JMethod createMethod(final Schema writerSchema, final Schema readerSchema, boolean read) {
    if (!Schema.Type.RECORD.equals(writerSchema.getType())) {
      throw new FastDeserializerGeneratorException("Methods are defined only for records, not for " + writerSchema.getType());
    }
    if (methodAlreadyDefined(writerSchema, readerSchema, read)) {
      throw new FastDeserializerGeneratorException("Method already exists for: " + AvroCompatibilityHelper.getSchemaFullName(writerSchema));
    }

    Schema effectiveSchema = readerSchema != null ? readerSchema : writerSchema;

    JType methodType = read ? schemaAssistant.classFromSchema(effectiveSchema) : codeModel.VOID;
    JMethod method = generatedClass.method(JMod.PUBLIC, methodType,
        getUniqueName("deserialize" + effectiveSchema.getName()));

    method._throws(IOException.class);
    method.param(Object.class, VAR_NAME_FOR_REUSE);
    method.param(Decoder.class, DECODER);

    (read ? deserializeMethodMap : skipMethodMap).put(getEffectiveMethodName(writerSchema, readerSchema), method);

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

  static void setFieldsPerPopulationMethod(int fieldCount) {
    FIELDS_PER_POPULATION_METHOD = fieldCount;
  }

  /**
   * String literals in Java classes cannot exceed length of 65535.
   * For such cases we break down the literal into chunks of valid lengths and use StringBuilder.
   */
  private static JExpression composeStringLiteral(JCodeModel codeModel, String input) {
    if (input.length() <= MAX_LENGTH_OF_STRING_LITERAL) {
      return JExpr.lit(input);
    }

    // need to compose the message using StringBuilder
    JInvocation stringBuilder = JExpr._new(codeModel.ref(StringBuilder.class));
    for (int pos = 0; pos < input.length();) {
      int endIndex = Math.min(pos + MAX_LENGTH_OF_STRING_LITERAL, input.length());
      String chunkedLiteral = input.substring(pos, endIndex);
      stringBuilder = stringBuilder.invoke("append").arg(chunkedLiteral);
      pos = endIndex;
    }
    return stringBuilder.invoke("toString");
  }
}
