package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.primitive.PrimitiveBooleanArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveDoubleArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveFloatArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.avroutil1.Enums;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;


public class SchemaAssistant {
  // The following constants are not available in avro-1.4
  public static final String CLASS_PROP = "java-class";
  public static final String KEY_CLASS_PROP = "java-key-class";
  public static final String STRING_PROP = "avro.java.string";
  public static final String STRING_TYPE_STRING = "String";

  private final JCodeModel codeModel;
  private final boolean useGenericTypes;
  private final JClass defaultStringType;
  private Set<Class<? extends Exception>> exceptionsFromStringable;
  /**
   * This is used to collect all the fully qualified classes being used by the
   * generated class.
   * The purpose of this bookkeeping is that in some framework, those classes
   * couldn't be found during compilation time, so we need to manually find
   * the libraries, which define those classes, and put them in the compile classpath.
   */
  private final Set<String> fullyQualifiedClassNameSet = new HashSet<>();
  /**
   * Whether this schema assistant is for serializer generation or not.
   * The reason to differentiate serializer from de-serializer is that for some Avro features, such as java string property,
   * it is not meaningful for serializer in generic mode.
   */
  private final boolean isForSerializer;

  public SchemaAssistant(JCodeModel codeModel, boolean useGenericTypes, Class defaultStringClass, boolean isForSerializer) {
    this.codeModel = codeModel;
    this.useGenericTypes = useGenericTypes;
    /**
     * Use {@link TreeSet} here to make sure the code gen is deterministic.
     */
    this.exceptionsFromStringable = new TreeSet<>(Comparator.comparing(Class::getCanonicalName));
    this.defaultStringType = codeModel.ref(defaultStringClass);
    this.isForSerializer = isForSerializer;
  }

  protected Set<String> getUsedFullyQualifiedClassNameSet() {
    return fullyQualifiedClassNameSet;
  }

  /* Complex type here means type that it have to handle other types inside itself. */
  public static boolean isComplexType(Schema schema) {
    switch (schema.getType()) {
      case MAP:
      case RECORD:
      case ARRAY:
      case UNION:
        return true;
      default:
        return false;
    }
  }

  public static boolean isPrimitive(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
        return true;
      default:
        return false;
    }
  }

  /**
   * Determines if a data type is capable of reuse
   *
   * @param schema of the data type in question
   * @return true if that data type is reusable (by mutating it), false otherwise
   */
  public static boolean isCapableOfReuse(Schema schema) {
    // TODO: Add special handling for Strings... {@link org.apache.avro.util.Utf8} is reusable, but {@link String} is not.
    switch (schema.getType()) {
      case BOOLEAN:
      case DOUBLE:
      case ENUM:
      case FLOAT:
      case INT:
      case LONG:
        return false;
      default:
        return true;
    }
  }

  public static String getSchemaFullName(Schema schema) {
    return AvroCompatibilityHelper.getSchemaFullName(schema);
  }

  /**
   * @return true if it is valid to call both {@link Schema#getName()} and {@link GenericContainer#getSchema()} on this data type
   */
  public static boolean isNamedTypeWithSchema(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        return true;
      case ENUM:
      case FIXED:
        /**
         * This is used to avoid `getSchema` call since in Avro-1.4, `getSchema` method is not available for Enum and Fixed.
         */
        return Utils.isAvro14() ? false : true;
      default:
        return false;
    }
  }

  /**
   * @return true if it is valid to call {@link Schema#getName()} on this data type
   */
  public static boolean isNamedType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return true;
      default:
        return false;
    }
  }

  public static boolean isStringable(Schema schema) {
    if (!Schema.Type.STRING.equals(schema.getType())) {
      throw new SchemaAssistantException("String schema expected!");
    }

    if (Utils.isAbleToSupportJavaStrings()) {
      return schema.getProp(CLASS_PROP) != null || schema.getProp(STRING_PROP) != null;
    } else {
      return false;
    }
  }

  public static boolean hasStringableKey(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException("Map schema expected!");
    }

    if (Utils.isAbleToSupportJavaStrings()) {
      return schema.getProp(KEY_CLASS_PROP) != null;
    } else {
      return false;
    }
  }

  public static String getTypeName (Schema schema) {
    Schema.Type schemaType = schema.getType();
    if (Schema.Type.RECORD.equals(schemaType)) {
      return schema.getName();
    } else if (Schema.Type.ARRAY.equals(schemaType)) {
      return "Array_of_" + getTypeName(schema.getElementType());
    } else if (Schema.Type.MAP.equals(schemaType)) {
      return "Map_of_" + getTypeName(schema.getValueType());
    } else {
      return schema.getType().name();
    }
  }

  public void resetExceptionsFromStringable() {
    exceptionsFromStringable = new HashSet<>();
  }

  public Set<Class<? extends Exception>> getExceptionsFromStringable() {
    return exceptionsFromStringable;
  }

  public void setExceptionsFromStringable(Set<Class<? extends Exception>> exceptionsFromStringable) {
    this.exceptionsFromStringable = exceptionsFromStringable;
  }

  private void extendExceptionsFromStringable(String className) {
    if (URL.class.getName().equals(className)) {
      exceptionsFromStringable.add(MalformedURLException.class);
    } else if (URI.class.getName().equals(className)) {
      exceptionsFromStringable.add(URISyntaxException.class);
    } else if (BigInteger.class.getName().equals(className) || BigDecimal.class.getName().equals(className)) {
      exceptionsFromStringable.add(NumberFormatException.class);
    }
  }


  private JClass valueClassFromMapSchema(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException("Map schema was expected, instead got:" + schema.getType()); //.getName());
    }

    return classFromSchema(schema.getValueType());
  }

  private JClass elementClassFromArraySchema(Schema schema) {
    if (!Schema.Type.ARRAY.equals(schema.getType())) {
      throw new SchemaAssistantException("Array schema was expected, instead got:" + schema.getType()); //.getName());
    }

    return classFromSchema(schema.getElementType());
  }

  private JClass classFromUnionSchema(final Schema schema) {
    if (!Schema.Type.UNION.equals(schema.getType())) {
      throw new SchemaAssistantException("Union schema was expected, instead got:" + schema.getType()); //.getName());
    }

    if (schema.getTypes().size() == 1) {
      return classFromSchema(schema.getTypes().get(0));
    }

    if (schema.getTypes().size() == 2) {
      if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
        return classFromSchema(schema.getTypes().get(1));
      } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
        return classFromSchema(schema.getTypes().get(0));
      }
    }

    return codeModel.ref(Object.class);
  }

  public JClass classFromSchema(Schema schema) {
    return classFromSchema(schema, true, false);
  }

  public JClass classFromSchema(Schema schema, boolean abstractType) {
    return classFromSchema(schema, abstractType, false);
  }

  public JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType) {
    return classFromSchema(schema, abstractType, rawType, false);
  }

  /* Note that settings abstractType and rawType are not passed to subcalls */
  public JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType, boolean primitiveList) {
    JClass outputClass;

    switch (schema.getType()) {

      case RECORD:
        if (useGenericTypes) {
          if (abstractType) {
            outputClass = codeModel.ref(IndexedRecord.class);
          } else {
            outputClass = codeModel.ref(GenericData.Record.class);
          }
        } else {
          outputClass = codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(schema));
        }
        break;

      case ARRAY:
        Class klass = null;
        if (primitiveList) {
          switch (schema.getElementType().getType()) {
            case BOOLEAN: klass = abstractType ? PrimitiveBooleanList.class : PrimitiveBooleanArrayList.class; break;
            case DOUBLE: klass = abstractType ? PrimitiveDoubleList.class : PrimitiveDoubleArrayList.class; break;
            /**
             * N.B.: FLOAT will get superseded in
             * {@link FastDeserializerGenerator#processArray(JVar, String, Schema, Schema, JBlock, FastDeserializerGeneratorBase.FieldAction, BiConsumer, Supplier)}
             */
            case FLOAT: klass = abstractType ? PrimitiveFloatList.class : PrimitiveFloatArrayList.class; break;
            case INT: klass = abstractType ? PrimitiveIntList.class : PrimitiveIntArrayList.class; break;
            case LONG: klass = abstractType ? PrimitiveLongList.class : PrimitiveLongArrayList.class; break;
            default: // no-op
          }
        }
        if (null == klass) {
          if (abstractType) {
            klass = List.class;
          } else if (useGenericTypes) {
            klass = GenericData.Array.class;
          } else {
            klass = ArrayList.class;
          }
        }
        outputClass = codeModel.ref(klass);

        if (!rawType && !(primitiveList && isPrimitive(schema.getElementType()))) {
          outputClass = outputClass.narrow(elementClassFromArraySchema(schema));
        }
        break;
      case MAP:
        if (!abstractType) {
          outputClass = codeModel.ref(HashMap.class);
        } else {
          outputClass = codeModel.ref(Map.class);
        }
        if (!rawType) {
          outputClass = outputClass.narrow(findStringClass(schema), valueClassFromMapSchema(schema));
        }
        break;
      case UNION:
        outputClass = classFromUnionSchema(schema);
        break;
      case ENUM:
        outputClass =
            useGenericTypes ? codeModel.ref(GenericEnumSymbol.class) : codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(schema));
        break;
      case FIXED:
        if (useGenericTypes) {
          outputClass =  codeModel.ref(abstractType ? GenericFixed.class : GenericData.Fixed.class);
        } else {
          outputClass = codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(schema));
        }
        break;
      case BOOLEAN:
        outputClass = codeModel.ref(Boolean.class);
        break;
      case DOUBLE:
        outputClass = codeModel.ref(Double.class);
        break;
      case FLOAT:
        outputClass = codeModel.ref(Float.class);
        break;
      case INT:
        outputClass = codeModel.ref(Integer.class);
        break;
      case LONG:
        outputClass = codeModel.ref(Long.class);
        break;
      case STRING:
        outputClass = findStringClass(schema);
        break;
      case BYTES:
        outputClass = codeModel.ref(ByteBuffer.class);
        break;
      default:
        throw new SchemaAssistantException("Incorrect request for " + schema.getType()); //.getName() + " class!");
    }

    /**
     * Exclude the narrowed type.
     * Essentially, for type: java.util.ArrayList<java.util.Map<org.apache.avro.util.Utf8>,
     * {@link JClass#erasure()} will return java.util.ArrayList, and that is the class, which can be located by
     * {@link Class#forName(String)}.
     */
    fullyQualifiedClassNameSet.add(outputClass.erasure().fullName());

    return outputClass;
  }

  /**
   * Infer String class for the passed schema.
   * If the current Avro lib is avro-1.4, this function will return default String type directly.
   * Otherwise, according to Avro specification, this function only supports the following property to specify
   * Stringable class while using Specific deserializer:
   * 1. {@link Schema.Type#STRING} : {@link #CLASS_PROP};
   * 2. {@link Schema.Type#MAP} :  {@link #KEY_CLASS_PROP};
   *
   * If the above property is absent for Specific deserializer or the current deserializer is generic,
   * this function will try to look at {@link #STRING_PROP} to decide whether it should use {@link String}
   * or the default String type.
   * @param schema a schema who's string impl class we want to infer
   * @return the string class used by given schema
   */
  public JClass findStringClass(Schema schema) {
    Schema.Type schemaType = schema.getType();
    if (!schemaType.equals(Schema.Type.STRING) && !schemaType.equals(Schema.Type.MAP)) {
      throw new SchemaAssistantException("Function `findStringClass` only supports schema types: " + Schema.Type.STRING + " and " + Schema.Type.MAP);
    }
    JClass outputClass = defaultStringType();
    /**
     * When generating fast-serializer in generic mode, those java string property doesn't have any effect.
     */
    if (useGenericTypes && isForSerializer) {
      return outputClass;
    }
    if (Utils.isAbleToSupportJavaStrings()) {
      String stringClassProp;
      if (schemaType.equals(Schema.Type.STRING)) {
        stringClassProp = schema.getProp(CLASS_PROP);
      } else {
        stringClassProp = schema.getProp(KEY_CLASS_PROP);
      }
      if (!useGenericTypes && null != stringClassProp) {
        outputClass = codeModel.ref(stringClassProp);
        extendExceptionsFromStringable(schema.getProp(CLASS_PROP));
      } else {
        String stringProp = schema.getProp(STRING_PROP);
        if (null != stringProp && stringProp.equals(STRING_TYPE_STRING)) {
          outputClass = codeModel.ref(String.class);
        }
      }
      if (!Utils.isAbleToSupportStringableProps() &&
          !outputClass.equals(codeModel.ref(String.class)) &&
          !outputClass.equals(codeModel.ref(Utf8.class))) {
        // This case could happen in Avro 1.6 when using a non-Utf8 and non-String class (e.g. URI, etc.), in which
        // case  Avro 1.6 ignores the unsupported class, and fast-avro should be compatible with that behavior.
        outputClass = defaultStringType();
      }
    }
    return outputClass;
  }

  protected JClass defaultStringType() {
    return defaultStringType;
  }

  public JExpression getEnumValueByName(Schema enumSchema, JExpression nameExpr, JExpression getSchemaExpr) {
    if (useGenericTypes) {
      if (Utils.isAvro14()) {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(nameExpr);
      } else {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchemaExpr).arg(nameExpr);
      }
    } else {
      return codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(enumSchema)).staticInvoke("valueOf").arg(nameExpr);
    }
  }

  public JExpression getEnumValueByIndex(Schema enumSchema, JExpression indexExpr, JExpression getSchemaExpr) {
    if (useGenericTypes) {
      /**
       * TODO: Consider ways to avoid instantiating a new {@link org.apache.avro.generic.GenericData.EnumSymbol}
       *       instance, e.g. can we pre-allocate one "canonical" enum instance for each ordinal and keep handing
       *       out the same one repeatedly, given that they are not mutable? */
      if (Utils.isAvro14()) {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class))
            .arg(getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr));
      } else {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class))
            .arg(getSchemaExpr)
            .arg(getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr));
      }
    } else {
      JClass enumClass = codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(enumSchema));
      return codeModel.ref(Enums.class).staticInvoke("getConstant").arg(enumClass.dotclass()).arg(indexExpr);
    }
  }

  public JExpression getFixedValue(Schema schema, JExpression fixedBytesExpr, JInvocation getSchemaExpr) {
    if (!useGenericTypes) {
      return JExpr._new(codeModel.ref(AvroCompatibilityHelper.getSchemaFullName(schema))).arg(fixedBytesExpr);
    } else {
      return JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchemaExpr).arg(fixedBytesExpr);
    }
  }

  public JExpression getStringableValue(Schema schema, JExpression stringExpr) {
    if (isStringable(schema)) {
      return JExpr._new(classFromSchema(schema)).arg(stringExpr);
    } else {
      return JExpr._new(defaultStringType()).arg(stringExpr);
    }
  }

  // TODO: this code should support primitive type promotions
  public int compatibleUnionSchemaIndex(Schema schema, Schema unionSchema) {
    for (int i = 0; i < unionSchema.getTypes().size(); i++) {
      Schema potentialCompatibleSchema = unionSchema.getTypes().get(i);
      if (potentialCompatibleSchema.getType().equals(schema.getType()) &&
              (!isNamedType(potentialCompatibleSchema) ||
                      AvroCompatibilityHelper.getSchemaFullName(potentialCompatibleSchema).equals(AvroCompatibilityHelper.getSchemaFullName(schema)) ||
                      potentialCompatibleSchema.getAliases().contains(AvroCompatibilityHelper.getSchemaFullName(schema)))) {
        return i;
      }
    }
    throw new SchemaAssistantException("No compatible schema found");
  }

  public Schema compatibleUnionSchema(Schema schema, Schema unionSchema) {
    return unionSchema.getTypes().get(compatibleUnionSchemaIndex(schema, unionSchema));
  }
}
