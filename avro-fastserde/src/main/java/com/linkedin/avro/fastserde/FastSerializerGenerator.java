package com.linkedin.avro.fastserde;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JVar;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;


public class FastSerializerGenerator<T> extends FastSerializerGeneratorBase<T> {

  private static final String ENCODER = "encoder";

  private final boolean useGenericTypes;
  private final Map<String, JMethod> serializeMethodMap = new HashMap<>();
  private final SchemaAssistant schemaAssistant;

  public FastSerializerGenerator(boolean useGenericTypes, Schema schema, File destination, ClassLoader classLoader,
      String compileClassPath) {
    super(schema, destination, classLoader, compileClassPath);
    this.useGenericTypes = useGenericTypes;
    this.schemaAssistant = new SchemaAssistant(codeModel, useGenericTypes);
  }

  @Override
  public FastSerializer<T> generateSerializer() {
    final String className = getClassName(schema, useGenericTypes ? "Generic" : "Specific");
    final JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

    try {
      serializerClass = classPackage._class(className);

      final JMethod serializeMethod = serializerClass.method(JMod.PUBLIC, void.class, "serialize");
      final JVar serializeMethodParam;

      JClass outputClass = classFromSchemaForSerializer(schema);
      serializerClass._implements(codeModel.ref(FastSerializer.class).narrow(outputClass));
      serializeMethodParam = serializeMethod.param(outputClass, "data");

      switch (schema.getType()) {
        case RECORD:
          processRecord(schema, serializeMethodParam, serializeMethod.body());
          break;
        case ARRAY:
          processArray(schema, serializeMethodParam, serializeMethod.body());
          break;
        case MAP:
          processMap(schema, serializeMethodParam, serializeMethod.body());
          break;
        default:
          throw new FastSerializerGeneratorException("Unsupported input schema type: " + schema.getType());
      }

      serializeMethod.param(codeModel.ref(Encoder.class), ENCODER);
      serializeMethod._throws(codeModel.ref(IOException.class));

      final Class<FastSerializer<T>> clazz = compileClass(className, schemaAssistant.getUsedFullyQualifiedClassNameSet());
      return clazz.newInstance();
    } catch (JClassAlreadyExistsException e) {
      throw new FastSerializerGeneratorException("Class: " + className + " already exists");
    } catch (Exception e) {
      throw new FastSerializerGeneratorException(e);
    }
  }

  private void processComplexType(Schema schema, JExpression valueExpr, JBlock body) {
    switch (schema.getType()) {
      case RECORD:
        processRecord(schema, valueExpr, body);
        break;
      case ARRAY:
        processArray(schema, valueExpr, body);
        break;
      case UNION:
        processUnion(schema, valueExpr, body);
        break;
      case MAP:
        processMap(schema, valueExpr, body);
        break;
      default:
        throw new FastSerializerGeneratorException("Not a complex schema type: " + schema.getType());
    }
  }

  private void processSimpleType(Schema schema, JExpression valueExpression, JBlock body) {
    switch (schema.getType()) {
      case ENUM:
        processEnum(schema, valueExpression, body);
        break;
      case FIXED:
        processFixed(schema, valueExpression, body);
        break;
      default:
        processPrimitive(schema, valueExpression, body);
        break;
    }
  }

  private void processRecord(final Schema recordSchema, JExpression recordExpr, final JBlock containerBody) {
    if (methodAlreadyDefined(recordSchema)) {
      containerBody.invoke(getMethod(recordSchema)).arg(recordExpr).arg(JExpr.direct(ENCODER));
      return;
    }
    JMethod method = createMethod(recordSchema);
    containerBody.invoke(getMethod(recordSchema)).arg(recordExpr).arg(JExpr.direct(ENCODER));

    JBlock body = method.body();
    recordExpr = method.listParams()[0];

    for (Schema.Field field : recordSchema.getFields()) {
      Schema fieldSchema = field.schema();
      if (SchemaAssistant.isComplexType(fieldSchema)) {
        JClass fieldClass = classFromSchemaForSerializer(fieldSchema);
        JVar containerVar = declareValueVar(field.name(), fieldSchema, body);
        JExpression valueExpression = JExpr.invoke(recordExpr, "get").arg(JExpr.lit(field.pos()));
        containerVar.init(JExpr.cast(fieldClass, valueExpression));

        processComplexType(fieldSchema, containerVar, body);
      } else {
        processSimpleType(fieldSchema, recordExpr.invoke("get").arg(JExpr.lit(field.pos())), body);
      }
    }
  }

  /**
   * Special handling for "String" type since the underlying data could be "String" or "Utf8".
   *
   * This is different from the de-serializer since Avro will always decode it into "Utf8".
   * @param fieldClass
   * @return
   */
  private JClass specialHandlingOfStringSchemaForSerializer(JClass fieldClass) {
    if (fieldClass.equals(codeModel.ref(Utf8.class))) {
      return codeModel.ref(CharSequence.class);
    }
    return fieldClass;
  }

  private JClass classFromSchemaForSerializer(Schema fieldSchema) {
    JClass fieldClass = schemaAssistant.classFromSchema(fieldSchema);
    return specialHandlingOfStringSchemaForSerializer(fieldClass);
  }

  private JClass classFromSchemaForSerializer(Schema fieldSchema, boolean abstractType) {
    JClass fieldClass = schemaAssistant.classFromSchema(fieldSchema, abstractType);
    return specialHandlingOfStringSchemaForSerializer(fieldClass);
  }

  private JClass classFromSchemaForSerializer(Schema fieldSchema, boolean abstractType, boolean rawType) {
    JClass fieldClass = schemaAssistant.classFromSchema(fieldSchema, abstractType, rawType);
    return specialHandlingOfStringSchemaForSerializer(fieldClass);
  }

  private void processArray(final Schema arraySchema, JExpression arrayExpr, JBlock body) {
    final JClass arrayClass = classFromSchemaForSerializer(arraySchema);
    body.invoke(JExpr.direct(ENCODER), "writeArrayStart");

    final JExpression emptyArrayCondition = arrayExpr.eq(JExpr._null()).cor(JExpr.invoke(arrayExpr, "isEmpty"));

    final JConditional emptyArrayIf = body._if(emptyArrayCondition);
    final JBlock emptyArrayBlock = emptyArrayIf._then();
    emptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));

    final JBlock nonEmptyArrayBlock = emptyArrayIf._else();
    nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.invoke(arrayExpr, "size"));
    final JForLoop forLoop = nonEmptyArrayBlock._for();
    final JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(JExpr.invoke(JExpr.cast(arrayClass, arrayExpr), "size")));
    forLoop.update(counter.incr());
    final JBlock forBody = forLoop.body();
    forBody.invoke(JExpr.direct(ENCODER), "startItem");

    final Schema elementSchema = arraySchema.getElementType();
    if (SchemaAssistant.isComplexType(elementSchema)) {
      JVar containerVar = declareValueVar(elementSchema.getName(), elementSchema, forBody);
      forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayClass, arrayExpr), "get").arg(counter));
      processComplexType(elementSchema, containerVar, forBody);
    } else {
      processSimpleType(elementSchema, arrayExpr.invoke("get").arg(counter), forBody);
    }
    body.invoke(JExpr.direct(ENCODER), "writeArrayEnd");
  }

  private void processMap(final Schema mapSchema, JExpression mapExpr, JBlock body) {

    final JClass mapClass = classFromSchemaForSerializer(mapSchema);

    JClass keyClass = specialHandlingOfStringSchemaForSerializer(schemaAssistant.keyClassFromMapSchema(mapSchema));

    body.invoke(JExpr.direct(ENCODER), "writeMapStart");

    final JExpression emptyMapCondition = mapExpr.eq(JExpr._null()).cor(JExpr.invoke(mapExpr, "isEmpty"));
    final JConditional emptyMapIf = body._if(emptyMapCondition);
    final JBlock emptyMapBlock = emptyMapIf._then();
    emptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));

    final JBlock nonEmptyMapBlock = emptyMapIf._else();
    nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.invoke(mapExpr, "size"));

    final JForEach mapKeysLoop = nonEmptyMapBlock.forEach(keyClass, getVariableName("key"),
        JExpr.invoke(JExpr.cast(mapClass, mapExpr), "keySet"));

    final JBlock forBody = mapKeysLoop.body();
    forBody.invoke(JExpr.direct(ENCODER), "startItem");

    JVar keyStringVar;
    if (SchemaAssistant.hasStringableKey(mapSchema)) {
      keyStringVar =
          forBody.decl(codeModel.ref(String.class), getVariableName("keyString"), mapKeysLoop.var().invoke("toString"));
    } else {
      keyStringVar = mapKeysLoop.var();
    }

    final Schema valueSchema = mapSchema.getValueType();

    forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(keyStringVar);

    JVar containerVar;
    if (SchemaAssistant.isComplexType(valueSchema)) {
      containerVar = declareValueVar(valueSchema.getName(), valueSchema, forBody);
      forBody.assign(containerVar, JExpr.invoke(JExpr.cast(mapClass, mapExpr), "get").arg(mapKeysLoop.var()));

      processComplexType(valueSchema, containerVar, forBody);
    } else {
      processSimpleType(valueSchema, mapExpr.invoke("get").arg(mapKeysLoop.var()), forBody);
    }
    body.invoke(JExpr.direct(ENCODER), "writeMapEnd");
  }

  /**
   * Avro-1.4 doesn't provide function: "getIndexNamed", so we just create the following function
   * with the similar logic, which will work with both Avro-1.4 and Avro-1.7.
   *
   * @param unionSchema
   * @param schema
   * @return
   */
  private Integer getIndexNamedForUnion(Schema unionSchema, Schema schema) {
    if (!unionSchema.getType().equals(Schema.Type.UNION)) {
      throw new RuntimeException("Union schema expected, but received: " + unionSchema.getType());
    }
    List<Schema> subSchemas = unionSchema.getTypes();
    int index = 0;
    for (Schema subSchema : subSchemas) {
      if (subSchema.getType().equals(schema.getType())) {
        return index;
      }
      index++;
    }
    throw new RuntimeException("Unknown schema: " + schema + " in union schema: " + unionSchema);
  }

  private void processUnion(final Schema unionSchema, JExpression unionExpr, JBlock body) {
    JConditional ifBlock = null;
    for (Schema schemaOption : unionSchema.getTypes()) {
      // Special handling for null
      if (Schema.Type.NULL.equals(schemaOption.getType())) {
        JExpression condition = unionExpr.eq(JExpr._null());
        ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
        JBlock thenBlock = ifBlock._then();
        thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
            .arg(JExpr.lit(getIndexNamedForUnion(unionSchema, schemaOption)));
        thenBlock.invoke(JExpr.direct(ENCODER), "writeNull");
        continue;
      }

      JClass optionClass = classFromSchemaForSerializer(schemaOption);
      JClass rawOptionClass = classFromSchemaForSerializer(schemaOption, true, true);
      JExpression condition = unionExpr._instanceof(rawOptionClass);
      if (useGenericTypes && SchemaAssistant.isNamedType(schemaOption)) {
        condition = condition.cand(JExpr.invoke(JExpr.lit(schemaOption.getFullName()), "equals")
            .arg(JExpr.invoke(JExpr.cast(optionClass, unionExpr), "getSchema").invoke("getFullName")));
      }
      ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
      JBlock thenBlock = ifBlock._then();
      thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
          .arg(JExpr.lit(getIndexNamedForUnion(unionSchema, schemaOption)));

      if (schemaOption.getType().equals(Schema.Type.UNION) || schemaOption.getType().equals(Schema.Type.NULL)) {
        throw new FastSerializerGeneratorException("Incorrect union subschema processing: " + schemaOption);
      }
      if (SchemaAssistant.isComplexType(schemaOption)) {
        processComplexType(schemaOption, JExpr.cast(optionClass, unionExpr), thenBlock);
      } else {
        processSimpleType(schemaOption, unionExpr, thenBlock);
      }
    }
  }

  private void processFixed(Schema fixedSchema, JExpression fixedValueExpression, JBlock body) {
    JClass fixedClass = classFromSchemaForSerializer(fixedSchema);
    body.invoke(JExpr.direct(ENCODER), "writeFixed")
        .arg(JExpr.invoke(JExpr.cast(fixedClass, fixedValueExpression), "bytes"));
  }

  private void processEnum(Schema enumSchema, JExpression enumValueExpression, JBlock body) {
    JClass enumClass = classFromSchemaForSerializer(enumSchema);
    JExpression enumValueCasted = JExpr.cast(enumClass, enumValueExpression);
    JExpression valueToWrite;
    if (useGenericTypes) {
      valueToWrite =
          JExpr.invoke(enumValueCasted.invoke("getSchema"), "getEnumOrdinal").arg(enumValueCasted.invoke("toString"));
    } else {
      valueToWrite = enumValueCasted.invoke("ordinal");
    }

    body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(valueToWrite);
  }

  private void processString(final Schema primitiveSchema, JExpression primitiveValueExpression, JBlock body) {
    String writeFunction = "writeString";
    JExpression encodeVar = JExpr.direct(ENCODER);
    if (!useGenericTypes && SchemaAssistant.isStringable(primitiveSchema)) {
      body.invoke(encodeVar, writeFunction).arg(primitiveValueExpression.invoke("toString"));
    } else {
      JConditional stringTypeCheck = body._if(primitiveValueExpression._instanceof(codeModel.ref(Utf8.class)));
      stringTypeCheck._then()
          .invoke(encodeVar, writeFunction)
          .arg(JExpr.cast(codeModel.ref(Utf8.class), primitiveValueExpression));
      stringTypeCheck._else().invoke(encodeVar, writeFunction).arg(primitiveValueExpression.invoke("toString"));
    }
  }

  private void processPrimitive(final Schema primitiveSchema, JExpression primitiveValueExpression, JBlock body) {
    String writeFunction;
    JClass primitiveClass = classFromSchemaForSerializer(primitiveSchema);
    JExpression castedValue = JExpr.cast(primitiveClass, primitiveValueExpression);
    switch (primitiveSchema.getType()) {
      case STRING:
        processString(primitiveSchema, primitiveValueExpression, body);
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
        throw new FastSerializerGeneratorException(
            "Unsupported primitive schema of type: " + primitiveSchema.getType());
    }

    body.invoke(JExpr.direct(ENCODER), writeFunction).arg(castedValue);
  }

  private JVar declareValueVar(final String name, final Schema schema, JBlock block) {
    if (SchemaAssistant.isComplexType(schema)) {
      return block.decl(classFromSchemaForSerializer(schema, true), getVariableName(StringUtils.uncapitalize(name)),
          JExpr._null());
    } else {
      throw new FastDeserializerGeneratorException("Incorrect container variable: " + schema.getType()); //.getName());
    }
  }

  private boolean methodAlreadyDefined(final Schema schema) {
    return !Schema.Type.RECORD.equals(schema.getType()) || serializeMethodMap.containsKey(schema.getFullName());
  }

  private JMethod getMethod(final Schema schema) {
    if (Schema.Type.RECORD.equals(schema.getType())) {
      if (methodAlreadyDefined(schema)) {
        return serializeMethodMap.get(schema.getFullName());
      }
      throw new FastSerializerGeneratorException("No method for schema: " + schema.getFullName());
    }
    throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
  }

  private JMethod createMethod(final Schema schema) {
    if (Schema.Type.RECORD.equals(schema.getType())) {
      if (!methodAlreadyDefined(schema)) {
        JMethod method =
            serializerClass.method(JMod.PUBLIC, codeModel.VOID, "serialize" + schema.getName() + nextUniqueInt());
        method._throws(IOException.class);
        method.param(classFromSchemaForSerializer(schema), "data");
        method.param(Encoder.class, ENCODER);

        method.annotate(SuppressWarnings.class).param("value", "unchecked");
        serializeMethodMap.put(schema.getFullName(), method);

        return method;
      } else {
        throw new FastSerializerGeneratorException("Method already exists for: " + schema.getFullName());
      }
    }
    throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
  }
}
