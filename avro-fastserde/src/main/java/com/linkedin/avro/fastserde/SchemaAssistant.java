package com.linkedin.avro.fastserde;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;


public class SchemaAssistant {
  // The folllowing constants are not available in avro-1.4
  public static final String CLASS_PROP = "java-class";
  public static final String KEY_CLASS_PROP = "java-key-class";

  private final JCodeModel codeModel;
  private final boolean useGenericTypes;
  private Set<Class<? extends Exception>> exceptionsFromStringable;

  public SchemaAssistant(JCodeModel codeModel, boolean useGenericTypes) {
    this.codeModel = codeModel;
    this.useGenericTypes = useGenericTypes;
    this.exceptionsFromStringable = new HashSet<>();
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

    if (Utils.isAvro14()) {
      return false;
    } else {
      return schema.getProp(CLASS_PROP) != null;
    }
  }

  public static boolean hasStringableKey(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException("Map schema expected!");
    }

    if (Utils.isAvro14()) {
      return false;
    } else {
      return schema.getProp(KEY_CLASS_PROP) != null;
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

  public JClass keyClassFromMapSchema(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException("Map schema was expected, instead got:" + schema.getType()); //.getName());
    }
    if (hasStringableKey(schema) && !useGenericTypes) {
      extendExceptionsFromStringable(schema.getProp(KEY_CLASS_PROP));
      return codeModel.ref(schema.getProp(KEY_CLASS_PROP));
    } else {
      return codeModel.ref(Utf8.class);
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

  /* Note that settings abstractType and rawType are not passed to subcalls */
  public JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType) {
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
          outputClass = codeModel.ref(schema.getFullName());
        }
        break;

      case ARRAY:
        if (abstractType) {
          outputClass = codeModel.ref(List.class);
        } else {
          if (useGenericTypes) {
            outputClass = codeModel.ref(GenericData.Array.class);
          } else {
            outputClass = codeModel.ref(ArrayList.class);
          }
        }
        if (!rawType) {
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
          outputClass = outputClass.narrow(keyClassFromMapSchema(schema), valueClassFromMapSchema(schema));
        }
        break;
      case UNION:
        outputClass = classFromUnionSchema(schema);
        break;
      case ENUM:
        outputClass =
            useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class) : codeModel.ref(schema.getFullName());
        break;
      case FIXED:
        outputClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class) : codeModel.ref(schema.getFullName());
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
        if (!Utils.isAvro14() && isStringable(schema) && !useGenericTypes) {
          outputClass = codeModel.ref(schema.getProp(CLASS_PROP));
          extendExceptionsFromStringable(schema.getProp(CLASS_PROP));
        } else {
          outputClass = codeModel.ref(Utf8.class);
        }
        break;
      case BYTES:
        outputClass = codeModel.ref(ByteBuffer.class);
        break;
      default:
        throw new SchemaAssistantException("Incorrect request for " + schema.getType()); //.getName() + " class!");
    }

    return outputClass;
  }

  public JExpression getEnumValueByName(Schema enumSchema, JExpression nameExpr, JInvocation getSchemaExpr) {
    if (useGenericTypes) {
      if (Utils.isAvro14()) {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(nameExpr);
      } else {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchemaExpr).arg(nameExpr);
      }
    } else {
      return codeModel.ref(enumSchema.getFullName()).staticInvoke("valueOf").arg(nameExpr);
    }
  }

  public JExpression getEnumValueByIndex(Schema enumSchema, JExpression indexExpr, JInvocation getSchemaExpr) {
    if (useGenericTypes) {
      if (Utils.isAvro14()) {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class))
            .arg(getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr));
      } else {
        return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class))
            .arg(getSchemaExpr)
            .arg(getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr));
      }
    } else {
      return codeModel.ref(enumSchema.getFullName()).staticInvoke("values").component(indexExpr);
    }
  }

  public JExpression getFixedValue(Schema schema, JExpression fixedBytesExpr, JInvocation getSchemaExpr) {
    if (!useGenericTypes) {
      return JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBytesExpr);
    } else {
      return JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchemaExpr).arg(fixedBytesExpr);
    }
  }

  public JExpression getStringableValue(Schema schema, JExpression stringExpr) {
    if (isStringable(schema)) {
      return JExpr._new(classFromSchema(schema)).arg(stringExpr);
    } else {
      return JExpr._new(codeModel.ref(Utf8.class)).arg(stringExpr);
    }
  }
}
