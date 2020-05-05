package com.linkedin.avro.fastserde;

import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDoLoop;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JStatement;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
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


public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {

  private static final String DECODER = "decoder";
  private static final String VAR_NAME_FOR_REUSE = "reuse";

  /**
   * This is sometimes passed into the reuse parameter,
   * and Avro treats null as a sentinel value indicating that it should
   * instantiate a new object instead of re-using.
   */
  private static final Supplier<JExpression> EMPTY_SUPPLIER = () -> JExpr._null();

  private boolean useGenericTypes;
  private JMethod schemaMapMethod;
  private JFieldVar schemaMapField;
  private Map<Long, Schema> schemaMap = new HashMap<>();
  private Map<Long, JVar> schemaVarMap = new HashMap<>();
  private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
  private Map<String, JMethod> skipMethodMap = new HashMap<>();
  private Map<JMethod, Set<Class<? extends Exception>>> exceptionFromMethodMap = new HashMap<>();
  private SchemaAssistant schemaAssistant;

  FastDeserializerGenerator(boolean useGenericTypes, Schema writer, Schema reader, File destination,
      ClassLoader classLoader, String compileClassPath) {
    super(writer, reader, destination, classLoader, compileClassPath);
    this.useGenericTypes = useGenericTypes;
    this.schemaAssistant = new SchemaAssistant(codeModel, useGenericTypes);
  }

  public FastDeserializer<T> generateDeserializer() {
    String className = getClassName(writer, reader, useGenericTypes ? "Generic" : "Specific");
    JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

    try {
      deserializerClass = classPackage._class(className);

      JVar readerSchemaVar = deserializerClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class, "readerSchema");
      JMethod constructor = deserializerClass.constructor(JMod.PUBLIC);
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
        schemaMapField =
            deserializerClass.field(JMod.PRIVATE, codeModel.ref(Map.class).narrow(Long.class).narrow(Schema.class),
                "readerSchemaMap");
        schemaMapMethod = deserializerClass.method(JMod.PRIVATE | JMod.FINAL, void.class, "schemaMap");
        constructor.body().invoke(schemaMapMethod);
        schemaMapMethod.body()
            .assign(schemaMapField,
                JExpr._new(codeModel.ref(HashMap.class).narrow(Long.class).narrow(Schema.class)));
        registerSchema(aliasedWriterSchema, readerSchemaVar);
      }

      JClass readerSchemaClass = schemaAssistant.classFromSchema(reader);
      JClass writerSchemaClass = schemaAssistant.classFromSchema(aliasedWriterSchema);

      deserializerClass._implements(codeModel.ref(FastDeserializer.class).narrow(writerSchemaClass));
      JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

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

  private void processSimpleType(Schema schema, JBlock methodBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putExpressionIntoParent, Supplier<JExpression> reuseSupplier) {
    switch (schema.getType()) {
      case ENUM:
        processEnum(schema, methodBody, action, putExpressionIntoParent);
        break;
      case FIXED:
        processFixed(schema, methodBody, action, putExpressionIntoParent, reuseSupplier);
        break;
      default:
        processPrimitive(schema, methodBody, action, putExpressionIntoParent, reuseSupplier);
        break;
    }
  }

  private void ifCodeGen(JBlock parentBody, JExpression condition, Consumer<JBlock> thenClosure) {
    JConditional ifCondition = parentBody._if(condition);
    thenClosure.accept(ifCondition._then());
  }


  private void ifCodeGen(JBlock parentBody, JExpression condition, Consumer<JBlock> thenClosure,
      Consumer<JBlock> elseClosure) {
    JConditional ifCondition = parentBody._if(condition);
    thenClosure.accept(ifCondition._then());
    elseClosure.accept(ifCondition._else());
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
        JExpression recordSchema = schemaMapField.invoke("get").arg(JExpr.lit(Utils.getSchemaFingerprint(recordWriterSchema)));
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
        processSimpleType(field.schema(), methodBody, action, putExpressionInRecord, fieldReuseSupplier);
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
              body.decl(defaultValueClass, getVariableName("default" + schema.getName()), valueInitializationExpr);
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

          valueVar = body.decl(defaultValueClass, getVariableName("defaultArray"), valueInitializationExpr);

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

          valueVar = body.decl(defaultValueClass, getVariableName("defaultMap"), valueInitializationExpr);

          // Avro-1.4 depends on an old jackson-mapper-asl-1.4.2, which requires the following typecast.
          for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) defaultValue).getFields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> mapEntry = it.next();
            JExpression mapKeyExpr;
            if (SchemaAssistant.hasStringableKey(schema)) {
              mapKeyExpr = JExpr._new(schemaAssistant.keyClassFromMapSchema(schema)).arg(mapEntry.getKey());
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
            JVar fixed = body.decl(fixedClass, getVariableName(schema.getName()));
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
    JVar unionIndex = body.decl(codeModel.INT, getVariableName("unionIndex"), JExpr.direct(DECODER + ".readIndex()"));
    JConditional ifBlock = null;
    for (int i = 0; i < unionSchema.getTypes().size(); i++) {
      Schema optionSchema = unionSchema.getTypes().get(i);
      Schema readerOptionSchema = null;
      FieldAction unionAction;

      if (Schema.Type.NULL.equals(optionSchema.getType())) {
        body._if(unionIndex.eq(JExpr.lit(i)))._then().directStatement(DECODER + ".readNull();");
        continue;
      }

      if (action.getShouldRead()) {
        readerOptionSchema = readerUnionSchema.getTypes().get(i);
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

      JExpression condition = unionIndex.eq(JExpr.lit(i));
      ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
      final JBlock thenBlock = ifBlock._then();

      JVar optionSchemaVar = null;
      if (useGenericTypes && unionAction.getShouldRead()) {
        JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(i));
        optionSchemaVar = declareSchemaVar(optionSchema, name + "OptionSchema", optionSchemaExpression);
      }

      if (SchemaAssistant.isComplexType(optionSchema)) {
        String optionName = name + "Option";
        if (Schema.Type.UNION.equals(optionSchema.getType())) {
          throw new FastDeserializerGeneratorException("Union cannot be sub-type of union!");
        }
        processComplexType(optionSchemaVar, optionName, optionSchema, readerOptionSchema, thenBlock, unionAction,
            putValueIntoParent, reuseSupplier);
      } else {
        processSimpleType(optionSchema, thenBlock, unionAction, putValueIntoParent, reuseSupplier);
      }
    }
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

    final JVar arrayVar = action.getShouldRead() ? declareValueVar(name, arraySchema, parentBody) : null;
    /**
     * Special optimization for float array by leveraging {@link PrimitiveFloatList}.
     */
    if (action.getShouldRead() && arraySchema.getElementType().getType().equals(Schema.Type.FLOAT)) {
      JClass primitiveFloatList = codeModel.ref(PrimitiveFloatList.class);
      JExpression readPrimitiveFloatArrayInvocation = primitiveFloatList.staticInvoke("readPrimitiveFloatArray").
          arg(reuseSupplier.get()).arg(JExpr.direct(DECODER));
      JExpression castedResult =
          JExpr.cast(codeModel.ref(List.class).narrow(Float.class), readPrimitiveFloatArrayInvocation);

      parentBody.assign(arrayVar, castedResult);
      putArrayIntoParent.accept(parentBody, arrayVar);
      return;
    }

    JClass arrayClass = schemaAssistant.classFromSchema(arraySchema, false);

    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getVariableName("chunkLen"), JExpr.direct(DECODER + ".readArrayStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlockForChunkLenCheck = conditional._then();
    if (action.getShouldRead()) {
      JVar reuse = declareValueVar(name + "Reuse", arraySchema, ifBlockForChunkLenCheck);

      ifCodeGen(ifBlockForChunkLenCheck,
          reuseSupplier.get()._instanceof(codeModel.ref(List.class)),
          thenBlock -> thenBlock.assign(reuse, JExpr.cast(codeModel.ref(List.class), reuseSupplier.get())));

      JInvocation newArrayExp = JExpr._new(arrayClass);
      if (useGenericTypes) {
        newArrayExp = newArrayExp.arg(JExpr.cast(codeModel.INT, chunkLen)).arg(getSchemaExpr(arraySchema));
      }
      JInvocation finalNewArrayExp = newArrayExp;
      // check whether the reuse is null or not
      ifCodeGen(ifBlockForChunkLenCheck,
          reuse.ne(JExpr.direct("null")),
          thenBlock -> {
            thenBlock.invoke(reuse, "clear");
            thenBlock.assign(arrayVar, reuse);
          },
          (elseBlock -> elseBlock.assign(arrayVar, finalNewArrayExp))
      );

      JBlock elseBlock = conditional._else();
      if (useGenericTypes) {
        elseBlock.assign(arrayVar, JExpr._new(arrayClass).arg(JExpr.lit(0)).arg(getSchemaExpr(arraySchema)));
      } else {
        elseBlock.assign(arrayVar, codeModel.ref(Collections.class).staticInvoke("emptyList"));
      }
    }

    JDoLoop doLoop = ifBlockForChunkLenCheck._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JVar elementSchemaVar = null;
    BiConsumer<JBlock, JExpression> putValueInArray = null;
    if (action.getShouldRead()) {
      putValueInArray = (block, expression) -> block.invoke(arrayVar, "add").arg(expression);
      if (useGenericTypes) {
        elementSchemaVar = declareSchemaVar(arraySchema.getElementType(), name + "ArrayElemSchema",
            arraySchemaVar.invoke("getElementType"));
      }
    }

    // Define element reuse variable here
    JVar elementReuseVar = forBody.decl(codeModel.ref(Object.class), getVariableName(name + "ArrayElementReuseVar"), JExpr._null());
    ifCodeGen(forBody,
        reuseSupplier.get()._instanceof(codeModel.ref(GenericArray.class)),
        then ->
            then.assign(elementReuseVar, JExpr.invoke(JExpr.cast(codeModel.ref(GenericArray.class), reuseSupplier.get()), "peek"))
    );
    Supplier<JExpression> elementReuseSupplier = () -> elementReuseVar;

    if (SchemaAssistant.isComplexType(arraySchema.getElementType())) {
      String elemName = name + "Elem";
      Schema readerArrayElementSchema = null;
      if (action.getShouldRead()) {
        readerArrayElementSchema = readerArraySchema.getElementType();
      }
      processComplexType(elementSchemaVar, elemName, arraySchema.getElementType(), readerArrayElementSchema, forBody,
          action, putValueInArray, elementReuseSupplier);
    } else {
      processSimpleType(arraySchema.getElementType(), forBody, action, putValueInArray, elementReuseSupplier);
    }
    doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));

    if (action.getShouldRead()) {
      putArrayIntoParent.accept(parentBody, arrayVar);
    }
  }

  private void processMap(JVar mapSchemaVar, final String name, final Schema mapSchema, final Schema readerMapSchema,
      JBlock parentBody, FieldAction action, BiConsumer<JBlock, JExpression> putMapIntoParent,
      Supplier<JExpression> reuseSupplier) {

    /**
     * Determine the action symbol for Map value. {@link ResolvingGrammarGenerator} generates
     * resolving grammar symbols with reversed order of production sequence. If this symbol is
     * a terminal, its production list will be <tt>null</tt>. Otherwise the production list
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

    final JVar mapVar = action.getShouldRead() ? declareValueVar(name, mapSchema, parentBody) : null;
    JVar chunkLen =
        parentBody.decl(codeModel.LONG, getVariableName("chunkLen"), JExpr.direct(DECODER + ".readMapStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlockForChunkLenCheck = conditional._then();

    if (action.getShouldRead()) {
      JVar reuse = declareValueVar(name + "Reuse", mapSchema, ifBlockForChunkLenCheck);

      // Check whether the reuse is a Map or not
      ifCodeGen(ifBlockForChunkLenCheck,
          reuseSupplier.get()._instanceof(codeModel.ref(Map.class)),
          thenBlock -> thenBlock.assign(reuse, JExpr.cast(codeModel.ref(Map.class), reuseSupplier.get())));

      // Check whether the reuse is null or not
      ifCodeGen(ifBlockForChunkLenCheck,
          reuse.ne(JExpr.direct("null")),
          thenBlock -> {
            thenBlock.invoke(reuse, "clear");
            thenBlock.assign(mapVar, reuse);
          },
          elseBlock -> elseBlock.assign(mapVar, JExpr._new(schemaAssistant.classFromSchema(mapSchema, false)))
      );

      JBlock elseBlock = conditional._else();
      elseBlock.assign(mapVar, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
    }

    JDoLoop doLoop = ifBlockForChunkLenCheck._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JClass keyClass = schemaAssistant.keyClassFromMapSchema(mapSchema);
    JExpression keyValueExpression = JExpr.direct(DECODER + ".readString(null)");
    if (SchemaAssistant.hasStringableKey(mapSchema)) {
      keyValueExpression = JExpr.direct(DECODER + ".readString(null).toString()");
      keyValueExpression = JExpr._new(keyClass).arg(keyValueExpression);
    }

    JVar key = forBody.decl(keyClass, getVariableName("key"), keyValueExpression);
    JVar mapValueSchemaVar = null;
    if (action.getShouldRead() && useGenericTypes) {
      mapValueSchemaVar =
          declareSchemaVar(mapSchema.getValueType(), name + "MapValueSchema", mapSchemaVar.invoke("getValueType"));
    }
    BiConsumer<JBlock, JExpression> putValueInMap = null;
    if (action.getShouldRead()) {
      putValueInMap = (block, expression) -> block.invoke(mapVar, "put").arg(key).arg(expression);
    }
    if (SchemaAssistant.isComplexType(mapSchema.getValueType())) {
      String valueName = name + "Value";
      Schema readerMapValueSchema = null;
      if (action.getShouldRead()) {
        readerMapValueSchema = readerMapSchema.getValueType();
      }
      processComplexType(mapValueSchemaVar, valueName, mapSchema.getValueType(), readerMapValueSchema, forBody, action,
          putValueInMap, EMPTY_SUPPLIER);
    } else {
      processSimpleType(mapSchema.getValueType(), forBody, action, putValueInMap, EMPTY_SUPPLIER);
    }
    doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));
    if (action.getShouldRead()) {
      putMapIntoParent.accept(parentBody, mapVar);
    }
  }

  private void processFixed(final Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putFixedIntoParent, Supplier<JExpression> reuseSupplier) {
    if (action.getShouldRead()) {
      JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getVariableName(schema.getName()), null);
      if (reuseSupplier.get().equals(JExpr._null())) {
        body.assign(fixedBuffer, JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));
      } else {
        /**
         * Here will check whether the length of the reused fixed is same as the one to be deserialized or not.
         * If not, here will initialize a new byte array to store it.
         */
        ifCodeGen(body,
            reuseSupplier.get()._instanceof(codeModel.ref(GenericFixed.class)).
                cand(JExpr.invoke(JExpr.cast(codeModel.ref(GenericFixed.class), reuseSupplier.get()), "bytes")
                    .ref("length")
                    .eq(JExpr.direct("" + schema.getFixedSize()))),
            thenBlock -> thenBlock.assign(fixedBuffer,
                JExpr.invoke(JExpr.cast(codeModel.ref(GenericFixed.class), reuseSupplier.get()), "bytes")),
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
        JVar fixed = body.decl(fixedClass, getVariableName(schema.getName()));
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
        JVar enumIndex = body.decl(codeModel.INT, getVariableName("enumIndex"), enumValueExpr);
        JClass enumClass = schemaAssistant.classFromSchema(schema);
        newEnum = body.decl(enumClass, getVariableName("enumValue"), JExpr._null());

        for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
          JExpression ithVal =
              schemaAssistant.getEnumValueByIndex(schema, JExpr.lit((Integer) enumAdjustAction.adjustments[i]),
                  getSchemaExpr(schema));
          body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum, ithVal);
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
        ifCodeGen(body,
            reuseSupplier.get()._instanceof(codeModel.ref("java.nio.ByteBuffer")),
            thenBlock -> putValueIntoParent.accept(thenBlock, JExpr.invoke(JExpr.direct(DECODER), "readBytes")
                .arg(JExpr.cast(codeModel.ref(ByteBuffer.class), reuseSupplier.get()))),
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
      if (reuseSupplier.get().equals(JExpr._null())) {
        JExpression readStringInvocation;

        if (schema.getType().equals(Schema.Type.STRING) && !useGenericTypes && SchemaAssistant.isStringable(schema)) {
          readStringInvocation = JExpr._new(schemaAssistant.classFromSchema(schema))
              .arg(JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr._null()).
                  invoke("toString"));
        } else {
          readStringInvocation = JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr._null());
        }
        putValueIntoParent.accept(body, readStringInvocation);
      } else {
        ifCodeGen(body,
            reuseSupplier.get()._instanceof(codeModel.ref("org.apache.avro.util.Utf8")),
            thenBlock -> {
              JExpression readStringInvocation;
              if (!useGenericTypes && schema.getType().equals(Schema.Type.STRING) && SchemaAssistant.isStringable(
                  schema)) {
                readStringInvocation = JExpr._new(schemaAssistant.classFromSchema(schema))
                    .arg(JExpr.invoke(JExpr.direct(DECODER), "readString").
                        arg(JExpr.cast(codeModel.ref(Utf8.class), reuseSupplier.get())).invoke("toString"));
              } else {
                readStringInvocation = JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr.cast(codeModel.ref(Utf8.class), reuseSupplier.get()));
              }
              putValueIntoParent.accept(thenBlock, readStringInvocation);
            },
            elseBlock -> {
              JExpression readStringInvocation;
              if (!useGenericTypes && schema.getType().equals(Schema.Type.STRING) && SchemaAssistant.isStringable(
                  schema)) {
                readStringInvocation = JExpr._new(schemaAssistant.classFromSchema(schema))
                    .arg(JExpr.invoke(JExpr.direct(DECODER), "readString").
                        arg(JExpr._null()).invoke("toString"));
              } else {
                readStringInvocation = JExpr.invoke(JExpr.direct(DECODER), "readString").arg(JExpr._null());
              }
              putValueIntoParent.accept(elseBlock, readStringInvocation);
            }
        );
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

  private JVar declareValueVar(final String name, final Schema schema, JBlock block) {
    if (SchemaAssistant.isComplexType(schema)) {
      return block.decl(schemaAssistant.classFromSchema(schema), getVariableName(StringUtils.uncapitalize(name)),
          JExpr._null());
    } else {
      throw new FastDeserializerGeneratorException("Only complex types allowed!");
    }
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
        JVar schemaVar = schemaMapMethod.body()
            .decl(codeModel.ref(Schema.class), getVariableName(StringUtils.uncapitalize(variableName)), getValueType);
        registerSchema(valueSchema, schemaId, schemaVar);
        schemaVarMap.put(schemaId, schemaVar);
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
      schemaMapMethod.body().invoke(schemaMapField, "put").arg(JExpr.lit(schemaId)).arg(schemaVar);
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
    JMethod method = deserializerClass.method(JMod.PUBLIC, read ? schemaClass : codeModel.VOID,
        getVariableName("deserialize" + schema.getName()));

    method._throws(IOException.class);
    method.param(Object.class, VAR_NAME_FOR_REUSE);
    method.param(Decoder.class, DECODER);

    (read ? deserializeMethodMap : skipMethodMap).put(schema.getFullName(), method);

    return method;
  }

  private JInvocation getSchemaExpr(Schema schema) {
    return useGenericTypes ? schemaMapField.invoke("get").arg(JExpr.lit(Utils.getSchemaFingerprint(schema))) : null;
  }
}
