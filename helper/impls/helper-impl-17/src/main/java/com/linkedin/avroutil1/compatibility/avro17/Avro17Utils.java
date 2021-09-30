/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.compatibility.StringPropertyUtils;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * utility code specific to avro 1.7
 */
public class Avro17Utils {
    private final static boolean IS_AT_LEAST_1_7_3 = isIsAtLeast173();
    private final static Method GET_JSON_PROPS_METHOD;
    private final static Method GET_JSON_PROP_METHOD;
    private final static Method ADD_JSON_PROP_METHOD;

    static {
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

    static boolean isIsAtLeast173() {
        //class org.apache.avro.JsonProperties was created as part of AVRO-1157 for 1.7.3
        //however, if we naively just test for its existence we risk finding it in some extra
        //avro jar at the end of the classpath, with the "dominant" avro being an older jar
        //at the beginning of the classpath (this is horrible, but such is life).
        //as such a safer approach is to look up class org.apache.avro.Schema (which exists
        //in all supported avro versions and so we assume originates from the "dominant" jar)
        //and see if it extends org.apache.avro.JsonProperties
        Class<? super Schema> parentOfSchema = Schema.class.getSuperclass();
        //noinspection RedundantIfStatement
        if ("org.apache.avro.JsonProperties".equals(parentOfSchema.getName())) {
            return true;
        }
        return false; //presumably the parent is "java.lang.Object"
    }

    static Method findNewerGetPropsMethod() {
        try {
            Class<?> jsonPropertiesClass = Class.forName("org.apache.avro.JsonProperties");
            return jsonPropertiesClass.getDeclaredMethod("getJsonProps");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Method findNewerGetPropMethod() {
        try {
            Class<?> jsonPropertiesClass = Class.forName("org.apache.avro.JsonProperties");
            return jsonPropertiesClass.getDeclaredMethod("getJsonProp", String.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Method findNewerAddPropMethod() {
        try {
            Class<?> jsonPropertiesClass = Class.forName("org.apache.avro.JsonProperties");
            return jsonPropertiesClass.getDeclaredMethod("addProp", String.class, JsonNode.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
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
        Map<String, String> strProps = schema.getProps();;
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
        if (ADD_JSON_PROP_METHOD != null) {
            try {
                for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
                    ADD_JSON_PROP_METHOD.invoke(field, entry.getKey(), entry.getValue());
                }
                return;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        //this must be < 1.7.3
        for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
            JsonNode jsonValue = entry.getValue();
            if (jsonValue.isTextual()) {
                field.addProp(entry.getKey(), jsonValue.getTextValue());
            }
        }
    }

    static void setProps(Schema schema, Map<String, JsonNode> jsonProps) {
        if (ADD_JSON_PROP_METHOD != null) {
            try {
                for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
                    ADD_JSON_PROP_METHOD.invoke(schema, entry.getKey(), entry.getValue());
                }
                return;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        //this must be < 1.7.3
        for (Map.Entry<String, JsonNode> entry : jsonProps.entrySet()) {
            JsonNode jsonValue = entry.getValue();
            if (jsonValue.isTextual()) {
                schema.addProp(entry.getKey(), jsonValue.getTextValue());
            }
        }
    }
}
