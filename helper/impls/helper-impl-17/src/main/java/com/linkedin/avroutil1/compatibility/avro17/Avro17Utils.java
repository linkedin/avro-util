/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.ClassLoaderUtil;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.compatibility.StringPropertyUtils;
import com.linkedin.avroutil1.compatibility.VersionDetectionUtil;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * utility code specific to avro 1.7
 */
public class Avro17Utils {
  private final static Logger LOG = LoggerFactory.getLogger(Avro17Utils.class);

  private final static boolean IS_AT_LEAST_1_7_3;
  private final static Class<?> JSONPROPERTIES_CLASS;
  private final static Method GET_JSON_PROPS_METHOD;
  private final static Method GET_JSON_PROP_METHOD;
  private final static Method ADD_JSON_PROP_METHOD;

  static {
    //class org.apache.avro.JsonProperties was created as part of AVRO-1157 for 1.7.3
    //however, if we naively just test for its existence we risk finding it in some extra
    //avro jar at the end of the classpath, with the "dominant" avro being an older jar
    //at the beginning of the classpath (this is horrible, but such is life).
    //as such a safer approach is to look up class org.apache.avro.Schema (which exists
    //in all supported avro versions and so we assume originates from the "dominant" jar)
    //and see if it extends org.apache.avro.JsonProperties
    Class<? super Schema> parentOfSchema = Schema.class.getSuperclass();
    if ("org.apache.avro.JsonProperties".equals(parentOfSchema.getName())) {
      JSONPROPERTIES_CLASS = parentOfSchema;
      VersionDetectionUtil.markUsedForCoreAvro(JSONPROPERTIES_CLASS);
      IS_AT_LEAST_1_7_3 = true;
    } else {
      JSONPROPERTIES_CLASS = null;
      IS_AT_LEAST_1_7_3 = false; //presumably the parent is "java.lang.Object"
    }

    //print warning about avro 1.7 soup if we find it
    try {
      Class<?> jsonPropertiesClass = ClassLoaderUtil.forName("org.apache.avro.JsonProperties");
      VersionDetectionUtil.markUsedForCoreAvro(jsonPropertiesClass);
      if (jsonPropertiesClass != JSONPROPERTIES_CLASS) {
        LOG.warn("multiple versions of avro 1.7 found on the classpath. sources of avro: {}",
            VersionDetectionUtil.uniqueSourcesForCoreAvro());
      }
    } catch (Exception ignored) {
      //ignored
    }

    if (IS_AT_LEAST_1_7_3) {
      GET_JSON_PROPS_METHOD = findNewerGetPropsMethod();
      GET_JSON_PROP_METHOD = findNewerGetPropMethod();
      ADD_JSON_PROP_METHOD = findNewerAddPropMethod();
    } else {
      GET_JSON_PROPS_METHOD = null;
      GET_JSON_PROP_METHOD = null;
      ADD_JSON_PROP_METHOD = null;
    }
  }

  public static boolean isIsAtLeast173() {
    return IS_AT_LEAST_1_7_3;
  }

  static Method findNewerGetPropsMethod() {
    try {
      return JSONPROPERTIES_CLASS.getDeclaredMethod("getJsonProps");
    } catch (Exception e) {
      String msg = "unable to locate expected method org.apache.avro.JsonProperties.getJsonProps(). "
          + "sources of avro classes are " + VersionDetectionUtil.uniqueSourcesForCoreAvro();
      throw new IllegalStateException(msg, e);
    }
  }

  static Method findNewerGetPropMethod() {
    try {
      return JSONPROPERTIES_CLASS.getDeclaredMethod("getJsonProp", String.class);
    } catch (Exception e) {
      String msg = "unable to locate expected method org.apache.avro.JsonProperties.getJsonProp(). "
          + "sources of avro classes are " + VersionDetectionUtil.uniqueSourcesForCoreAvro();
      throw new IllegalStateException(msg, e);
    }
  }

  static Method findNewerAddPropMethod() {
    try {
      return JSONPROPERTIES_CLASS.getDeclaredMethod("addProp", String.class, JsonNode.class);
    } catch (Exception e) {
      String msg =
          "unable to locate expected method org.apache.avro.JsonProperties.addProp(). " + "sources of avro classes are "
              + VersionDetectionUtil.uniqueSourcesForCoreAvro();
      throw new IllegalStateException(msg, e);
    }
  }

  static String getJsonProp(Schema.Field field, String name) {
    if (GET_JSON_PROP_METHOD != null) {  // >= Avro 1.7.3
      JsonNode node;
      try {
        node = (JsonNode) GET_JSON_PROP_METHOD.invoke(field, name);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return Jackson1Utils.toJsonString(node);
    } else {  // <= Avro 1.7.2
      return StringPropertyUtils.getFieldPropAsJsonString(field, name);
    }
  }

  static void setJsonProp(Schema.Field field, String name, String value, boolean strict) {
    if (ADD_JSON_PROP_METHOD != null) {  // >= Avro 1.7.3
      JsonNode node = Jackson1Utils.toJsonNode(value, strict);
      try {
        ADD_JSON_PROP_METHOD.invoke(field, name, node);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else {  // <= Avro 1.7.2
      StringPropertyUtils.setFieldPropFromJsonString(field, name, value, strict);
    }
  }

  static String getJsonProp(Schema schema, String name) {
    if (GET_JSON_PROP_METHOD != null) {  // >= Avro 1.7.3
      JsonNode node;
      try {
        node = (JsonNode) GET_JSON_PROP_METHOD.invoke(schema, name);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return Jackson1Utils.toJsonString(node);
    } else {  // <= Avro 1.7.2
      return StringPropertyUtils.getSchemaPropAsJsonString(schema, name);
    }
  }

  static void setJsonProp(Schema schema, String name, String value, boolean strict) {
    if (ADD_JSON_PROP_METHOD != null) {  // >= Avro 1.7.3
      JsonNode node = Jackson1Utils.toJsonNode(value, strict);
      try {
        ADD_JSON_PROP_METHOD.invoke(schema, name, node);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else {  // <= Avro 1.7.2
      StringPropertyUtils.setSchemaPropFromJsonString(schema, name, value, strict);
    }
  }

  static Map<String, JsonNode> getProps(Schema.Field field) {
    if (GET_JSON_PROPS_METHOD != null) {
      try {
        //noinspection unchecked
        return (Map<String, JsonNode>) GET_JSON_PROPS_METHOD.invoke(field);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    //this must be < 1.7.3
    @SuppressWarnings("deprecation")
    Map<String, String> strProps = field.props();
    if (strProps == null) {
      return null;
    }
    Map<String, JsonNode> jsonProps = new HashMap<>(strProps.size());
    for (Map.Entry<String, String> entry : strProps.entrySet()) {
      jsonProps.put(entry.getKey(), new TextNode(entry.getValue()));
    }
    return jsonProps;
  }

  static Map<String, JsonNode> getProps(Schema schema) {
    if (GET_JSON_PROPS_METHOD != null) {
      try {
        //noinspection unchecked
        return (Map<String, JsonNode>) GET_JSON_PROPS_METHOD.invoke(schema);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    //this must be < 1.7.3
    @SuppressWarnings("deprecation")
    Map<String, String> strProps = schema.getProps();
    ;
    if (strProps == null) {
      return null;
    }
    Map<String, JsonNode> jsonProps = new HashMap<>(strProps.size());
    for (Map.Entry<String, String> entry : strProps.entrySet()) {
      jsonProps.put(entry.getKey(), new TextNode(entry.getValue()));
    }
    return jsonProps;
  }

  static void setProps(Schema.Field field, Map<String, JsonNode> jsonProps) {
    if (IS_AT_LEAST_1_7_3) {
      try {
        for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
          ADD_JSON_PROP_METHOD.invoke(field, entry.getKey(), entry.getValue());
        }
        return;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else {
      //this must be < 1.7.3
      for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
        JsonNode jsonValue = entry.getValue();
        if (jsonValue.isTextual()) {
          field.addProp(entry.getKey(), jsonValue.getTextValue());
        }
      }
    }
  }

  static void setProps(Schema schema, Map<String, JsonNode> jsonProps) {
    if (IS_AT_LEAST_1_7_3) {
      try {
        for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
          ADD_JSON_PROP_METHOD.invoke(schema, entry.getKey(), entry.getValue());
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else {
      for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
        JsonNode jsonValue = entry.getValue();
        if (jsonValue.isTextual()) {
          schema.addProp(entry.getKey(), jsonValue.getTextValue());
        }
      }
    }
  }

  static boolean sameJsonProperties(Schema.Field a, Schema.Field b, boolean compareStringProps,
      boolean compareNonStringProps, Set<String> jsonPropNamesToIgnore) {
    if (IS_AT_LEAST_1_7_3) {
      return sameJsonPropertiesNewer17(a, b, compareStringProps, compareNonStringProps, jsonPropNamesToIgnore);
    } else {
      //older avro 1.7
      if (compareNonStringProps) {
        throw new IllegalArgumentException(
            "older avro 1.7 does not preserve non-string props and so cannot compare them");
      }
      if (a == null || b == null) {
        return false;
      }
      if (!compareStringProps) {
        return true;
      }
      Map<String, String> unfilteredJsonPropsA = a.props();
      Map<String, String> unfilteredJsonPropsB = a.props();

      if (jsonPropNamesToIgnore == null) {
        return Objects.equals(unfilteredJsonPropsA, unfilteredJsonPropsB);
      } else {
        Map<String, String> aProps = new HashMap<>();
        Map<String, String> bProps = new HashMap<>();
        for (Map.Entry<String, String> entry : unfilteredJsonPropsA.entrySet()) {
          if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
            aProps.put(entry.getKey(), entry.getValue());
          }
        }
        for (Map.Entry<String, String> entry : unfilteredJsonPropsB.entrySet()) {
          if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
            bProps.put(entry.getKey(), entry.getValue());
          }
        }
        return Objects.equals(aProps, bProps);
      }
    }
  }

  static boolean sameJsonProperties(Schema a, Schema b, boolean compareStringProps, boolean compareNonStringProps,
      Set<String> jsonPropNamesToIgnore) {
    if (IS_AT_LEAST_1_7_3) {
      return sameJsonPropertiesNewer17(a, b, compareStringProps, compareNonStringProps, jsonPropNamesToIgnore);
    } else {
      //older avro 1.7
      if (compareNonStringProps) {
        throw new IllegalArgumentException(
            "older avro 1.7 does not preserve non-string props and so cannot compare them");
      }
      if (a == null || b == null) {
        return false;
      }
      if (!compareStringProps) {
        return true;
      }

      Map<String, String> unfilteredJsonPropsA = a.getProps();
      Map<String, String> unfilteredJsonPropsB = a.getProps();

      if (jsonPropNamesToIgnore == null) {
        return Objects.equals(unfilteredJsonPropsA, unfilteredJsonPropsB);
      } else {
        Map<String, String> aProps = new HashMap<>();
        Map<String, String> bProps = new HashMap<>();
        for (Map.Entry<String, String> entry : unfilteredJsonPropsA.entrySet()) {
          if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
            aProps.put(entry.getKey(), entry.getValue());
          }
        }
        for (Map.Entry<String, String> entry : unfilteredJsonPropsB.entrySet()) {
          if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
            bProps.put(entry.getKey(), entry.getValue());
          }
        }
        return Objects.equals(aProps, bProps);
      }
    }
  }

  private static boolean sameJsonPropertiesNewer17(Object jsonPropertiesA, Object jsonPropertiesB,
      boolean compareStringProps, boolean compareNonStringProps, Set<String> jsonPropNamesToIgnore) {
    Map<String, JsonNode> unfilteredJsonPropsA;
    Map<String, JsonNode> unfilteredJsonPropsB;
    try {
      //noinspection unchecked
      unfilteredJsonPropsA = (Map<String, JsonNode>) GET_JSON_PROPS_METHOD.invoke(jsonPropertiesA);
      //noinspection unchecked
      unfilteredJsonPropsB = (Map<String, JsonNode>) GET_JSON_PROPS_METHOD.invoke(jsonPropertiesB);
    } catch (Exception e) {
      throw new IllegalStateException("unable to access JsonProperties", e);
    }

    if (jsonPropNamesToIgnore == null) {
      return Jackson1Utils.compareJsonProperties(unfilteredJsonPropsA, unfilteredJsonPropsB, compareStringProps,
          compareNonStringProps);
    } else {
      Map<String, JsonNode> aProps = new HashMap<>();
      Map<String, JsonNode> bProps = new HashMap<>();
      for (Map.Entry<String, JsonNode> entry : unfilteredJsonPropsA.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          aProps.put(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, JsonNode> entry : unfilteredJsonPropsB.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          bProps.put(entry.getKey(), entry.getValue());
        }
      }
      return Jackson1Utils.compareJsonProperties(aProps, bProps, compareStringProps, compareNonStringProps);
    }
  }
}
