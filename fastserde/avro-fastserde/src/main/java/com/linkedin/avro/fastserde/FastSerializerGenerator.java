package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JVar;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.function.Supplier;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;


public class FastSerializerGenerator<T, U extends GenericData> extends FastSerdeBase<U> {

  private static int FIELDS_PER_RECORD_SERIALIZATION_METHOD = 20;

  private static final String ENCODER = "encoder";
  private static final String VAR_NAME_FOR_CUSTOMIZATION = "customization";
  protected final Schema schema;

  private final Map<String, JMethod> serializeMethodMap = new HashMap<>();

  /**
   * Enum schema mapping for Avro-1.4 to record schema id and corresponding schema JVar.
   */
  private final Map<Integer, JVar> enumSchemaVarMap = new HashMap<>();

  static void setFieldsPerRecordSerializationMethod(int fieldCount) {
    FIELDS_PER_RECORD_SERIALIZATION_METHOD = fieldCount;
  }

  public FastSerializerGenerator(boolean useGenericTypes, Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath, U modelData) {
    super("serialization", useGenericTypes, CharSequence.class, destination, classLoader, compileClassPath, modelData, true);
    this.schema = schema;
  }

  public static String getClassName(Schema schema, String description) {
    int schemaId = Math.abs(Utils.getSchemaFingerprint(schema));
    String typeName = SchemaAssistant.getTypeName(schema);
    return typeName + SEP + description + "Serializer" + SEP + schemaId;
  }

  public FastSerializer<T> generateSerializer() {
    final String className = getClassName(schema, useGenericTypes ? "Generic" : "Specific");
    final JPackage classPackage = codeModel._package(generatedPackageName);

    try {
      generatedClass = classPackage._class(className);
      injectConversionClasses();

      final JMethod serializeMethod = generatedClass.method(JMod.PUBLIC, void.class, "serialize");
      final JVar serializeMethodParam;

      JClass outputClass = schemaAssistant.classFromSchema(schema);
      generatedClass._implements(codeModel.ref(FastSerializer.class).narrow(outputClass));
      serializeMethodParam = serializeMethod.param(outputClass, "data");
      final Supplier<JExpression> customizationSupplier = () -> JExpr.direct(VAR_NAME_FOR_CUSTOMIZATION);

      switch (schema.getType()) {
        case RECORD:
          processRecord(schema, serializeMethodParam, serializeMethod.body(), customizationSupplier);
          break;
        case ARRAY:
          processArray(schema, serializeMethodParam, serializeMethod.body(), customizationSupplier);
          break;
        case MAP:
          processMap(schema, serializeMethodParam, serializeMethod.body(), customizationSupplier);
          break;
        default:
          throw new FastSerdeGeneratorException("Unsupported input schema type: " + schema.getType());
      }

      serializeMethod.param(codeModel.ref(Encoder.class), ENCODER);
      serializeMethod.param(codeModel.ref(DatumWriterCustomization.class), VAR_NAME_FOR_CUSTOMIZATION);
      serializeMethod._throws(codeModel.ref(IOException.class));

      @SuppressWarnings("unchecked")
      final Class<FastSerializer<T>> clazz = compileClass(className, schemaAssistant.getUsedFullyQualifiedClassNameSet());

      return clazz.getConstructor().newInstance();
    } catch (JClassAlreadyExistsException e) {
      throw new FastSerdeGeneratorException("Class: " + className + " already exists");
    } catch (Exception e) {
      throw new FastSerdeGeneratorException(e);
    }
  }

  private void processComplexType(Schema schema, JExpression valueExpr, JBlock body, Supplier<JExpression> customizationSupplier) {
    switch (schema.getType()) {
      case RECORD:
        processRecord(schema, valueExpr, body, customizationSupplier);
        break;
      case ARRAY:
        processArray(schema, valueExpr, body, customizationSupplier);
        break;
      case UNION:
        processUnion(schema, valueExpr, body, customizationSupplier);
        break;
      case MAP:
        processMap(schema, valueExpr, body, customizationSupplier);
        break;
      default:
        throw new FastSerdeGeneratorException("Not a complex schema type: " + schema.getType());
    }
  }

  private void processSimpleType(Schema schema, JExpression valueExpression, JBlock body) {
    processSimpleType(schema, valueExpression, body, true);
  }

  private void processSimpleType(Schema schema, JExpression valueExpression, JBlock body, boolean cast) {
    switch (schema.getType()) {
      case ENUM:
        processEnum(schema, valueExpression, body);
        break;
      case FIXED:
        processFixed(schema, valueExpression, body);
        break;
      default:
        processPrimitive(schema, valueExpression, body, cast);
        break;
    }
  }

  private void processRecord(final Schema recordSchema, JExpression recordExpr, final JBlock containerBody, Supplier<JExpression> customizationSupplier) {
    if (methodAlreadyDefined(recordSchema)) {
      containerBody.invoke(getMethod(recordSchema)).arg(recordExpr).arg(JExpr.direct(ENCODER)).arg(customizationSupplier.get());
      return;
    }
    JMethod method = createMethod(recordSchema);
    containerBody.invoke(getMethod(recordSchema)).arg(recordExpr).arg(JExpr.direct(ENCODER)).arg(customizationSupplier.get());

    JBlock methodBody = method.body();
    recordExpr = method.listParams()[0];

    int fieldCount = 0;
    JBlock popMethodBody = methodBody;

    for (Schema.Field field : recordSchema.getFields()) {
      /*
       * We roll the serialization method for very large records, the initial fields are kept in the outer
       * method as original to maintain performance for smaller records
        */
      fieldCount++;
      if (fieldCount % FIELDS_PER_RECORD_SERIALIZATION_METHOD == 0) {
        JMethod  popMethod = generatedClass.method(JMod.PRIVATE, codeModel.VOID, getUniqueName("serialize_" + StringUtils.capitalize(recordSchema.getName())));
        popMethod._throws(IOException.class);
        popMethod.param(schemaAssistant.classFromSchema(recordSchema), "data");
        popMethod.param(Encoder.class, ENCODER);
        popMethod.param(DatumWriterCustomization.class, VAR_NAME_FOR_CUSTOMIZATION);
        popMethod.annotate(SuppressWarnings.class).param("value", "unchecked");

        popMethodBody = popMethod.body();
        methodBody.invoke(popMethod).arg(recordExpr).arg(JExpr.direct(ENCODER)).arg(customizationSupplier.get());
      }

      Schema fieldSchema = field.schema();
      JInvocation fieldValueGetter = recordExpr.invoke("get").arg(JExpr.lit(field.pos()));

      if (SchemaAssistant.isComplexType(fieldSchema)) {
        JClass fieldClass = schemaAssistant.classFromSchema(fieldSchema);
        JVar containerVar = popMethodBody.decl(fieldClass, getUniqueName(field.name()), JExpr.cast(fieldClass, fieldValueGetter));
        processComplexType(fieldSchema, containerVar, popMethodBody, customizationSupplier);
      } else {
        processSimpleType(fieldSchema, fieldValueGetter, popMethodBody);
      }
    }
  }

  private void processArray(final Schema arraySchema, JExpression arrayExpr, JBlock body, Supplier<JExpression> customizationSupplier) {
    final JClass arrayClass = schemaAssistant.classFromSchema(arraySchema);
    body.invoke(JExpr.direct(ENCODER), "writeArrayStart");

    final JExpression emptyArrayCondition = arrayExpr.eq(JExpr._null()).cor(JExpr.invoke(arrayExpr, "isEmpty"));

    ifCodeGen(body, emptyArrayCondition, then1 -> {
      then1.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
    }, else1 -> {
      else1.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.invoke(arrayExpr, "size"));

      if (SchemaAssistant.isPrimitive(arraySchema.getElementType())) {
        // Trick added to support logical types, e.g. (List<LocalDate> instanceof PrimitiveIntList) - can't compile
        JVar arrayVar = else1.decl(codeModel.ref(Object.class), getUniqueName("array"), arrayExpr);
        JClass primitiveListInterface = schemaAssistant.classFromSchema(arraySchema, true, false, true);
        final JExpression primitiveListCondition = arrayVar._instanceof(primitiveListInterface);

        ifCodeGen(else1, primitiveListCondition, then2 -> {
          final JVar primitiveList = declareValueVar("primitiveList", arraySchema, then2, true, false, true)
              .init(JExpr.cast(primitiveListInterface, arrayVar));
          if (arraySchema.getElementType().getType().equals(Schema.Type.FLOAT)) {
            /**
             * Check whether it is an instance of {@link BufferBackedPrimitiveFloatList} or not.
             */
            JClass bufferBackedPrimitiveFloatListClass = codeModel.ref(BufferBackedPrimitiveFloatList.class);
            final JExpression bufferBackedPrimitiveFloatListCondition = primitiveList._instanceof(bufferBackedPrimitiveFloatListClass);
            ifCodeGen(then2, bufferBackedPrimitiveFloatListCondition, then3 -> {
              final JVar bufferBackedPrimitiveFloatList = then3.decl(bufferBackedPrimitiveFloatListClass,
                  "bufferBackedPrimitiveFloatList", JExpr.cast(bufferBackedPrimitiveFloatListClass, primitiveList));
              then3.invoke(bufferBackedPrimitiveFloatList, "writeFloats").arg(JExpr.direct(ENCODER));
            }, else3 -> {
              processArrayElementLoop(arraySchema, arrayClass, primitiveList, else3, "getPrimitive", customizationSupplier);
            });
          } else {
            processArrayElementLoop(arraySchema, arrayClass, primitiveList, then2, "getPrimitive", customizationSupplier);
          }
        }, else2 -> {
          processArrayElementLoop(arraySchema, arrayClass, arrayExpr, else2, "get", customizationSupplier);
        });
      } else {
        processArrayElementLoop(arraySchema, arrayClass, arrayExpr, else1, "get", customizationSupplier);
      }
    });
    body.invoke(JExpr.direct(ENCODER), "writeArrayEnd");
  }

  private void processArrayElementLoop(final Schema arraySchema, final JClass arrayClass, JExpression arrayExpr, JBlock body, String getMethodName, Supplier<JExpression> customizationSupplier) {
    final JForLoop forLoop = body._for();
    final JVar counter = forLoop.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(JExpr.invoke(arrayExpr, "size")));
    forLoop.update(counter.incr());
    final JBlock forBody = forLoop.body();
    forBody.invoke(JExpr.direct(ENCODER), "startItem");

    final Schema elementSchema = arraySchema.getElementType();
    if (SchemaAssistant.isComplexType(elementSchema)) {
      JVar containerVar = declareValueVar(elementSchema.getName(), elementSchema, forBody);
      forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayClass, arrayExpr), getMethodName).arg(counter));
      processComplexType(elementSchema, containerVar, forBody, customizationSupplier);
    } else {
      processSimpleType(elementSchema, arrayExpr.invoke(getMethodName).arg(counter), forBody, false);
    }
  }

  private void processMap(final Schema mapSchema, JExpression mapExpr, JBlock body, Supplier<JExpression> customizationSupplier) {
    final JClass mapClass = schemaAssistant.classFromSchema(mapSchema);
    JClass keyClass = schemaAssistant.findStringClass(mapSchema);

    body.invoke(JExpr.invoke(customizationSupplier.get(),"getCheckMapTypeFunction"),"apply").arg(mapExpr);

    body.invoke(JExpr.direct(ENCODER), "writeMapStart");

    final JExpression emptyMapCondition = mapExpr.eq(JExpr._null()).cor(JExpr.invoke(mapExpr, "isEmpty"));
    final JConditional emptyMapIf = body._if(emptyMapCondition);
    final JBlock emptyMapBlock = emptyMapIf._then();
    emptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));

    final JBlock nonEmptyMapBlock = emptyMapIf._else();
    nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.invoke(mapExpr, "size"));

    final JForEach mapKeysLoop = nonEmptyMapBlock.forEach(keyClass, getUniqueName("key"),
        JExpr.invoke(JExpr.cast(mapClass, mapExpr), "keySet"));

    final JBlock forBody = mapKeysLoop.body();
    forBody.invoke(JExpr.direct(ENCODER), "startItem");

    JVar keyStringVar;
    if (SchemaAssistant.hasStringableKey(mapSchema)) {
      keyStringVar =
          forBody.decl(codeModel.ref(String.class), getUniqueName("keyString"), mapKeysLoop.var().invoke("toString"));
    } else {
      keyStringVar = mapKeysLoop.var();
    }

    final Schema valueSchema = mapSchema.getValueType();

    forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(keyStringVar);

    JVar containerVar;
    if (SchemaAssistant.isComplexType(valueSchema)) {
      containerVar = declareValueVar(valueSchema.getName(), valueSchema, forBody);
      forBody.assign(containerVar, JExpr.invoke(JExpr.cast(mapClass, mapExpr), "get").arg(mapKeysLoop.var()));

      processComplexType(valueSchema, containerVar, forBody, customizationSupplier);
    } else {
      processSimpleType(valueSchema, mapExpr.invoke("get").arg(mapKeysLoop.var()), forBody);
    }
    body.invoke(JExpr.direct(ENCODER), "writeMapEnd");
  }

  /**
   * Avro-1.4 doesn't provide function: "getIndexNamed", so we just create the following function
   * with the similar logic, which will work with both Avro-1.4 and Avro-1.7.
   */
  private Integer getIndexNamedForUnion(Schema unionSchema, Schema schema) {
    if (!unionSchema.getType().equals(Schema.Type.UNION)) {
      throw new RuntimeException("Union schema expected, but received: " + unionSchema.getType());
    }
    List<Schema> subSchemas = unionSchema.getTypes();
    String schemaFullName = SchemaAssistant.getSchemaFullName(schema);
    int index = 0;
    for (Schema subSchema : subSchemas) {
      if (SchemaAssistant.getSchemaFullName(subSchema).equals(schemaFullName)) {
        return index;
      }
      index++;
    }
    throw new RuntimeException("Unknown schema: " + schema + " in union schema: " + unionSchema);
  }

  private void processUnion(final Schema unionSchema, final JExpression unionExpr, final JBlock body, Supplier<JExpression> customizationSupplier) {
    JConditional ifBlock = null;

    for (Schema schemaOption : unionSchema.getTypes()) {
      if (Schema.Type.NULL.equals(schemaOption.getType())) {
        /*
         * We always handle the null branch of the union first, otherwise, it leads to a bug in the
         * case where there is an optional field where the null is the second branch of the union.
         */
        JExpression condition = unionExpr.eq(JExpr._null());
        ifBlock = body._if(condition);
        JBlock thenBlock = ifBlock._then();
        thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
            .arg(JExpr.lit(getIndexNamedForUnion(unionSchema, schemaOption)));
        thenBlock.invoke(JExpr.direct(ENCODER), "writeNull");
        break;
      }
    }

    for (Schema schemaOption : unionSchema.getTypes()) {
      if (Schema.Type.NULL.equals(schemaOption.getType())) {
        /*
         * Since we've already added code to process the null branch, we can skip it when processing
         * the other types.
         */
        continue;
      }

      JClass optionClass = schemaAssistant.classFromSchema(schemaOption);
      JClass rawOptionClass = schemaAssistant.classFromSchema(schemaOption, true, true);
      JClass optionLogicalTypeClass = logicalTypeEnabled(schemaOption)
              ? codeModel.ref(((Conversion<?>) schemaAssistant.getConversion(schemaOption.getLogicalType())).getConvertedType())
              : null;

      JExpression condition;
      /*
       * In Avro-1.4, neither GenericEnumSymbol nor GenericFixed has associated schema, so we don't expect to see
       * two or more Enum types or two or more Fixed types in the same Union in generic mode since the writer couldn't
       * differentiate the Enum types or the Fixed types, but those scenarios are well-supported in Avro-1.7 or above since
       * both of them have associated 'Schema', so the serializer could recognize the right type
       * by checking the associated 'Schema' in generic mode.
       */
      if (useGenericTypes && SchemaAssistant.isNamedTypeWithSchema(schemaOption)) {
        if (optionLogicalTypeClass != null && schemaOption.getType() == Schema.Type.FIXED) {
          condition = unionExpr._instanceof(optionLogicalTypeClass);
        } else {
          String schemaFullName = AvroCompatibilityHelper.getSchemaFullName(schemaOption);
          condition = unionExpr._instanceof(rawOptionClass).cand(JExpr.invoke(JExpr.lit(schemaFullName), "equals")
            .arg(JExpr.invoke(JExpr.cast(optionClass, unionExpr), "getSchema").invoke("getFullName")));
        }
      } else {
        if (unionExpr instanceof JVar && ((JVar)unionExpr).type().equals(rawOptionClass)) {
          condition = null;
        } else if (optionLogicalTypeClass != null) {
          condition = unionExpr._instanceof(optionLogicalTypeClass);
        } else {
          condition = unionExpr._instanceof(rawOptionClass);
        }
      }
      JBlock unionTypeProcessingBlock;
      if (condition == null) {
        unionTypeProcessingBlock = ifBlock != null ? ifBlock._else() : body;
      } else {
        ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
        unionTypeProcessingBlock = ifBlock._then();
      }

      unionTypeProcessingBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
          .arg(JExpr.lit(getIndexNamedForUnion(unionSchema, schemaOption)));

      if (schemaOption.getType().equals(Schema.Type.UNION) || schemaOption.getType().equals(Schema.Type.NULL)) {
        throw new FastSerdeGeneratorException("Incorrect union subschema processing: " + schemaOption);
      }

      if (SchemaAssistant.isComplexType(schemaOption)) {
        processComplexType(schemaOption, JExpr.cast(optionClass, unionExpr), unionTypeProcessingBlock, customizationSupplier);
      } else {
        processSimpleType(schemaOption, unionExpr, unionTypeProcessingBlock);
      }
    }
  }

  private void processFixed(Schema fixedSchema, JExpression fixedValueExpression, JBlock body) {
    JClass fixedClass = schemaAssistant.classFromSchema(fixedSchema, true, false, false, false);
    JExpression fixedValueToWrite;

    if (schemaAssistant.logicalTypeEnabled(fixedSchema)) {
      fixedValueToWrite = generateConversionToRawType(fixedSchema, fixedValueExpression, body);
    } else {
      fixedValueToWrite = fixedValueExpression;
    }

    body.invoke(JExpr.direct(ENCODER), "writeFixed")
        .arg(JExpr.invoke(JExpr.cast(fixedClass, fixedValueToWrite), "bytes"));
  }

  private void processEnum(Schema enumSchema, JExpression enumValueExpression, JBlock body) {
    JClass enumClass = schemaAssistant.classFromSchema(enumSchema);
    JExpression enumValueCasted = JExpr.cast(enumClass, enumValueExpression);
    JVar valueToWrite = body.decl(codeModel.INT, getUniqueName("valueToWrite"));

    if (useGenericTypes) {
      JVar enumValue = body.decl(codeModel.ref(Object.class), getUniqueName("enumValue"), enumValueExpression);
      JClass enumSymbolClass = codeModel.ref(GenericData.EnumSymbol.class);
      JExpression castEnumValueToEnumSymbol = JExpr.cast(enumSymbolClass, enumValue);
      JExpression schemaExpression;
      JExpression enumValueToStringExpr;

      if (Utils.isAvro14()) {
        /*
         * In Avro-1.4, there is no way to infer/extract enum schema from {@link org.apache.avro.generic.GenericData.EnumSymbol},
         * so the serializer needs to record the schema id and the corresponding {@link org.apache.avro.Schema.EnumSchema},
         * and maintain a mapping between the schema id and EnumSchema JVar for future use.
         */
        schemaExpression = enumSchemaVarMap.computeIfAbsent(Utils.getSchemaFingerprint(enumSchema), fingerprint ->
            generatedClass.field(
                JMod.PRIVATE | JMod.FINAL,
                Schema.class,
                getUniqueName(enumSchema.getName() + "EnumSchema"),
                codeModel.ref(Schema.class).staticInvoke("parse").arg(enumSchema.toString()))
        );
        enumValueToStringExpr = enumValueCasted.invoke("toString");
      } else {
        schemaExpression = castEnumValueToEnumSymbol.invoke("getSchema");
        enumValueToStringExpr = castEnumValueToEnumSymbol.invoke("toString");
      }

      ifCodeGen(body, enumValue._instanceof(codeModel.ref(Enum.class)),
              thenBlock -> thenBlock.assign(valueToWrite,
                      JExpr.invoke(JExpr.cast(codeModel.ref(Enum.class), enumValue), "ordinal")),
              elseBlock -> elseBlock.assign(valueToWrite,
                      JExpr.invoke(schemaExpression, "getEnumOrdinal").arg(enumValueToStringExpr)));
    } else {
      valueToWrite.init(enumValueCasted.invoke("ordinal"));
    }

    body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(valueToWrite);
  }

  private void processString(final Schema primitiveSchema, JExpression primitiveValueExpression, JBlock body) {
    String writeFunction = "writeString";
    JExpression encodeVar = JExpr.direct(ENCODER);
    if (!useGenericTypes && SchemaAssistant.isStringable(primitiveSchema)) {
      if (primitiveValueExpression instanceof JVar
          && ((JVar) primitiveValueExpression).type().equals(codeModel.ref(String.class))) {
        body.invoke(encodeVar, writeFunction).arg(primitiveValueExpression);
      } else {
        body.invoke(encodeVar, writeFunction).arg(primitiveValueExpression.invoke("toString"));
      }
    } else {
      JConditional stringTypeCheck = body._if(primitiveValueExpression._instanceof(codeModel.ref(Utf8.class)));
      stringTypeCheck._then()
          .invoke(encodeVar, writeFunction)
          .arg(JExpr.cast(codeModel.ref(Utf8.class), primitiveValueExpression));
      stringTypeCheck._else().invoke(encodeVar, writeFunction).arg(primitiveValueExpression.invoke("toString"));
    }
  }

  private void processPrimitive(final Schema primitiveSchema, JExpression primitiveValueExpression, JBlock body, boolean cast) {
    JClass primitiveClass = schemaAssistant.classFromSchema(primitiveSchema, true, false, false, false);

    String writeFunction;
    JExpression writeFunctionArgument;

    if (logicalTypeEnabled(primitiveSchema)) {
      JVar convertedValueVar = generateConversionToRawType(primitiveSchema, primitiveValueExpression, body);
      writeFunctionArgument = JExpr.cast(primitiveClass, convertedValueVar);
    } else {
      writeFunctionArgument = cast ? JExpr.cast(primitiveClass, primitiveValueExpression) : primitiveValueExpression;
    }

    switch (primitiveSchema.getType()) {
      case STRING:
        processString(primitiveSchema, writeFunctionArgument, body);
        return;
      case BYTES:
        writeFunction = "writeBytes";
        break;
      case INT:
        writeFunction = "writeInt";
        break;
      case LONG:
        writeFunction = "writeLong";
        break;
      case FLOAT:
        writeFunction = "writeFloat";
        break;
      case DOUBLE:
        writeFunction = "writeDouble";
        break;
      case BOOLEAN:
        writeFunction = "writeBoolean";
        break;
      default:
        throw new FastSerdeGeneratorException(
            "Unsupported primitive schema of type: " + primitiveSchema.getType());
    }

    body.invoke(JExpr.direct(ENCODER), writeFunction).arg(writeFunctionArgument);
  }

  private JVar generateConversionToRawType(Schema schemaWithLogicalType, JExpression initialExpression, JBlock body) {
    JVar convertedValue = body.decl(codeModel.ref(Object.class), getUniqueName("convertedValue"), initialExpression);
    JFieldRef schemaFieldRef = injectLogicalTypeSchema(schemaWithLogicalType);

    body.assign(convertedValue, codeModel.ref(Conversions.class)
            .staticInvoke("convertToRawType")
            .arg(convertedValue)
            .arg(schemaFieldRef)
            .arg(schemaFieldRef.invoke("getLogicalType"))
            .arg(getConversionRef(schemaWithLogicalType.getLogicalType())));

    return convertedValue;
  }

  private boolean methodAlreadyDefined(final Schema schema) {
    return !Schema.Type.RECORD.equals(schema.getType()) || serializeMethodMap.containsKey(AvroCompatibilityHelper.getSchemaFullName(schema));
  }

  private JMethod getMethod(final Schema schema) {
    if (Schema.Type.RECORD.equals(schema.getType())) {
      if (methodAlreadyDefined(schema)) {
        return serializeMethodMap.get(AvroCompatibilityHelper.getSchemaFullName(schema));
      }
      throw new FastSerdeGeneratorException("No method for schema: " + AvroCompatibilityHelper.getSchemaFullName(schema));
    }
    throw new FastSerdeGeneratorException("No method for schema type: " + schema.getType());
  }

  private JMethod createMethod(final Schema schema) {
    if (Schema.Type.RECORD.equals(schema.getType())) {
      if (!methodAlreadyDefined(schema)) {
        JMethod method = generatedClass.method(
            JMod.PUBLIC,
            codeModel.VOID,
            getUniqueName("serialize" + StringUtils.capitalize(schema.getName())));
        method._throws(IOException.class);
        method.param(schemaAssistant.classFromSchema(schema), "data");
        method.param(Encoder.class, ENCODER);
        method.param(DatumWriterCustomization.class, VAR_NAME_FOR_CUSTOMIZATION);

        method.annotate(SuppressWarnings.class).param("value", "unchecked");
        serializeMethodMap.put(AvroCompatibilityHelper.getSchemaFullName(schema), method);

        return method;
      } else {
        throw new FastSerdeGeneratorException("Method already exists for: " + AvroCompatibilityHelper.getSchemaFullName(schema));
      }
    }
    throw new FastSerdeGeneratorException("No method for schema type: " + schema.getType());
  }
}
