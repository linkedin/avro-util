/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroPrimitiveSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.CodeLocation;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.model.TextLocation;
import com.linkedin.avroutil1.parser.Located;
import com.linkedin.avroutil1.parser.exceptions.AvroSyntaxException;
import com.linkedin.avroutil1.parser.exceptions.JsonParseException;
import com.linkedin.avroutil1.parser.exceptions.ParseException;
import com.linkedin.avroutil1.parser.jsonpext.JsonArrayExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonNumberExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonObjectExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonPUtil;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderWithLocations;
import com.linkedin.avroutil1.parser.jsonpext.JsonStringExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;
import com.linkedin.avroutil1.util.Util;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParsingException;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * parses avro schemas out of input assumed to be avsc.
 * for the avsc specification see https://avro.apache.org/docs/current/spec.html#schemas
 *
 * differences between this parser and the vanilla avro parser:
 * <ul>
 *     <li>
 *         the avsc specification has no notion of imports - all nested schemas must be defined inline.
 *         this parser allows for "references" (types that are not defined anywhere in the current avsc)
 *         to be supplied from other avsc sources or context - effectively allowing imports
 *     </li>
 *     <li>
 *         the avsc specification does not allow for forward references, we do.
 *     </li>
 * </ul>
 */
public class AvscParser {

    public AvscParseResult parse(String avsc) {
        JsonReaderExt jsonReader = new JsonReaderWithLocations(new StringReader(avsc), null);
        JsonObjectExt root;
        AvscParseContext context = new AvscParseContext(avsc);
        AvscParseResult result = new AvscParseResult();
        try {
            root = jsonReader.readObject();
        } catch (JsonParsingException e) {
            result.recordError(new JsonParseException("json parse error at " + e.getLocation(), e));
            return result;
        } catch (Exception e) {
            result.recordError(new JsonParseException("unknown json parse error", e));
            return result;
        }

        try {
            parseSchemaDeclOrRef(root, context, true);
            context.resolveReferences();
            result.recordParseComplete(context);
        } catch (Exception parseIssue) {
            result.recordError(parseIssue);
        }

        return result;
    }

    /**
     * parses a schema (or a reference to one by FQCN) out of a given json node
     * @param node json node (which is a schema declaration or reference)
     * @param context context for this parse operation
     * @param topLevel true if this schema is the "top-level" schema for this context
     *                 (for example outer-most schema in an avsc file)
     * @return the schema defined by the input json node (which may be a reference)
     * @throws ParseException
     */
    private SchemaOrRef parseSchemaDeclOrRef (
            JsonValueExt node,
            AvscParseContext context,
            boolean topLevel
    ) throws ParseException {
        JsonValue.ValueType nodeType = node.getValueType();
        AvroType avroType;
        AvroSchema definedSchema;
        TextLocation startLocation = Util.convertLocation(node.getStartLocation());
        TextLocation endLocation = Util.convertLocation(node.getEndLocation());
        CodeLocation codeLocation = new CodeLocation(context.getUri(), startLocation, endLocation);
        switch (nodeType) {
            case STRING: //primitive or ref
                JsonStringExt stringNode = (JsonStringExt) node;
                String typeString = stringNode.getString();
                avroType = AvroType.fromJson(typeString);
                //TODO - screen for reserved words??
                if (avroType == null) {
                    //assume it's a ref
                    return new SchemaOrRef(codeLocation, typeString);
                }
                if (avroType.isPrimitive()) {
                    return new SchemaOrRef(codeLocation, AvroPrimitiveSchema.forType(codeLocation, avroType));
                }
                //if we got here it means we found something like "record" as a type literal. which is not valid syntax
                throw new AvroSyntaxException("Illegal avro type \"" + typeString + "\" at " + stringNode.getStartLocation()
                        + ". Expecting a primitive type, an inline type definition or a reference the the fullname of a named type defined elsewhere");
            case OBJECT: //record/enum/fixed/array/map/error
                JsonObjectExt objectNode = (JsonObjectExt) node;
                Located<String> typeStr = getRequiredString(objectNode, "type", () -> "it is a schema declaration");
                avroType = AvroType.fromJson(typeStr.getValue());
                if (avroType == null) {
                    throw new AvroSyntaxException("unknown avro type \"" + typeStr.getValue() + "\" at " + typeStr.getLocation() + ". expecting \"record\", \"enum\" or \"fixed\"");
                }
                Located<String> nameStr = null;
                Located<String> namespaceStr = null;
                Located<String> docStr = null;
                String namespace = context.getCurrentNamespace(); // != null
                boolean namespaceChanged = false;
                String doc = null;
                if (avroType.isNamed()) {
                    nameStr = getRequiredString(objectNode, "name", () -> avroType + " is a named type");

                    namespaceStr = getOptionalString(objectNode, "namespace");
                    //check if context namespace changed
                    if (namespaceStr != null) {
                        String newNamespace = namespaceStr.getValue();
                        if (!namespace.equals(newNamespace)) {
                            context.pushNamespace(newNamespace);
                            namespaceChanged = true;
                            namespace = newNamespace;
                        }
                    }

                    //technically the avro spec does not allow "doc" on type fixed, but screw that
                    docStr = getOptionalString(objectNode, "doc");
                    if (docStr != null) {
                        doc = docStr.getValue();
                    }
                }

                switch (avroType) {
                    case RECORD:
                        //noinspection ConstantConditions
                        AvroRecordSchema recordSchema = new AvroRecordSchema(
                                codeLocation,
                                nameStr.getValue(), // != null
                                namespace,
                                doc);
                        JsonArrayExt fieldsNode = getRequiredArray(objectNode, "fields", () -> "all avro records must have fields");
                        List<AvroSchemaField> fields = new ArrayList<>(fieldsNode.size());
                        for (int fieldNum = 0; fieldNum < fieldsNode.size(); fieldNum++) {
                            JsonValueExt fieldDeclNode = (JsonValueExt) fieldsNode.get(fieldNum); //!=null
                            JsonValue.ValueType fieldNodeType = fieldDeclNode.getValueType();
                            if (fieldNodeType != JsonValue.ValueType.OBJECT) {
                                throw new AvroSyntaxException("field " + fieldNum + " for record " + nameStr.getValue() + " at "
                                        + fieldDeclNode.getStartLocation() + " expected to be an OBJECT, not a "
                                        + JsonPUtil.describe(fieldNodeType) + " (" + fieldDeclNode + ")");
                            }
                            TextLocation fieldStartLocation = Util.convertLocation(fieldDeclNode.getStartLocation());
                            TextLocation fieldEndLocation = Util.convertLocation(fieldDeclNode.getEndLocation());
                            CodeLocation fieldCodeLocation = new CodeLocation(context.getUri(), fieldStartLocation, fieldEndLocation);
                            JsonObjectExt fieldDecl = (JsonObjectExt) fieldDeclNode;
                            Located<String> fieldName = getRequiredString(fieldDecl, "name", () -> "all record fields must have a name");
                            JsonValueExt fieldTypeNode = getRequiredNode(fieldDecl, "type", () -> "all record fields must have a type");
                            SchemaOrRef fieldSchema = parseSchemaDeclOrRef(fieldTypeNode, context, false);
                            JsonValueExt fieldDefaultValueNode = fieldDecl.get("default");
                            //TODO - validate and store default values
                            AvroSchemaField field = new AvroSchemaField(fieldCodeLocation, fieldName.getValue(), null, fieldSchema);
                            fields.add(field);
                        }
                        recordSchema.setFields(fields);
                        definedSchema = recordSchema;
                        break;
                    case ENUM:
                        JsonArrayExt symbolsNode = getRequiredArray(objectNode, "symbols", () -> "all avro enums must have symbols");
                        List<String> symbols = new ArrayList<>(symbolsNode.size());
                        for (int ordinal = 0; ordinal < symbolsNode.size(); ordinal++) {
                            JsonValueExt symbolNode = (JsonValueExt) symbolsNode.get(ordinal);
                            JsonValue.ValueType symbolNodeType = symbolNode.getValueType();
                            if (symbolNodeType != JsonValue.ValueType.STRING) {
                                throw new AvroSyntaxException("symbol " + ordinal + " for enum " + nameStr.getValue() + " at "
                                        + symbolNode.getStartLocation() + " expected to be a STRING, not a "
                                        + JsonPUtil.describe(symbolNodeType) + " (" + symbolNode + ")");
                            }
                            symbols.add(symbolNode.toString());
                        }
                        String defaultSymbol = null;
                        Located<String> defaultStr = getOptionalString(objectNode, "default");
                        if (defaultStr != null) {
                            defaultSymbol = defaultStr.getValue();
                            if (!symbols.contains(defaultSymbol)) {
                                throw new AvroSyntaxException("enum " + nameStr.getValue() + " has a default value of " + defaultSymbol
                                + " at " + defaultStr.getLocation() + " which is not in its list of symbols (" + symbols + ")");
                            }
                        }
                        definedSchema = new AvroEnumSchema(
                                codeLocation,
                                nameStr.getValue(), // != null
                                namespace,
                                doc,
                                symbols,
                                defaultSymbol);
                        break;
                    case FIXED:
                        JsonValueExt sizeNode = getRequiredNode(objectNode, "size", () -> "fixed types must have a size property");
                        if (sizeNode.getValueType() != JsonValue.ValueType.NUMBER || !(((JsonNumberExt) sizeNode).isIntegral())) {
                            throw new AvroSyntaxException("size for fixed " + nameStr.getValue() + " at "
                                    + sizeNode.getStartLocation() + " expected to be an INTEGER, not a "
                                    + JsonPUtil.describe(sizeNode.getValueType()) + " (" + sizeNode + ")");
                        }
                        int fixedSize = ((JsonNumberExt) sizeNode).intValue();
                        definedSchema = new AvroFixedSchema(
                                codeLocation,
                                nameStr.getValue(), // != null
                                namespace,
                                doc,
                                fixedSize
                        );
                        break;
                    case ARRAY:
                        JsonValueExt arrayItemsNode = getRequiredNode(objectNode, "items", () -> "array declarations must have an items property");
                        SchemaOrRef arrayItemSchema = parseSchemaDeclOrRef(arrayItemsNode, context, false);
                        definedSchema = new AvroArraySchema(codeLocation, arrayItemSchema);
                        break;
                    case MAP:
                        JsonValueExt mapValuesNode = getRequiredNode(objectNode, "values", () -> "map declarations must have a values property");
                        SchemaOrRef mapValueSchema = parseSchemaDeclOrRef(mapValuesNode, context, false);
                        definedSchema = new AvroMapSchema(codeLocation, mapValueSchema);
                        break;
                    default:
                        throw new IllegalStateException("unhandled: " + avroType + " at " + startLocation);
                }
                //TODO - parse json props

                context.defineSchema(new Located<>(definedSchema, startLocation), topLevel);
                if (namespaceChanged) {
                    context.popNamespace();
                }
                return new SchemaOrRef(codeLocation, definedSchema);
            case ARRAY:  //union
                JsonArrayExt arrayNode = (JsonArrayExt) node;
                List<SchemaOrRef> unionTypes = new ArrayList<>(arrayNode.size());
                for (int i = 0; i < arrayNode.size(); i++) {
                    JsonValueExt unionNode = (JsonValueExt) arrayNode.get(i);
                    SchemaOrRef type = parseSchemaDeclOrRef(unionNode, context, false);
                    unionTypes.add(type);
                }
                AvroUnionSchema unionSchema = new AvroUnionSchema(codeLocation);
                unionSchema.setTypes(unionTypes);
                definedSchema = unionSchema;
                context.defineSchema(new Located<>(definedSchema, startLocation), topLevel);
                return new SchemaOrRef(codeLocation, definedSchema);
            default:
                throw new IllegalArgumentException("dont know how to parse a schema out ot " + nodeType + " at " + node.getStartLocation());
        }
    }

    private JsonValueExt getRequiredNode(JsonObjectExt node, String prop, Supplier<String> reasonSupplier) {
        JsonValueExt val = node.get(prop);
        if (val == null) {
            String reason = reasonSupplier == null ? "" : (" because " + reasonSupplier.get());
            throw new AvroSyntaxException("json object at " + node.getStartLocation() + " is expected to have a "
                    + "\"" + prop + "\" property" + reason);
        }
        return val;
    }

    private JsonArrayExt getRequiredArray(JsonObjectExt node, String prop, Supplier<String> reasonSupplier) {
        JsonArrayExt optional = getOptionalArray(node, prop);
        if (optional == null) {
            String reason = reasonSupplier == null ? "" : (" because " + reasonSupplier.get());
            throw new AvroSyntaxException("json object at " + node.getStartLocation() + " is expected to have a \""
                    + prop + "\" property" + reason);
        }
        return optional;
    }

    private JsonArrayExt getOptionalArray(JsonObjectExt node, String prop) {
        JsonValueExt val = node.get(prop);
        if (val == null) {
            return null;
        }
        JsonValue.ValueType valType = val.getValueType();
        if (valType != JsonValue.ValueType.ARRAY) {
            throw new AvroSyntaxException("\"" + prop + "\" property at " + val.getStartLocation()
                    + " is expected to be a string, not a " + JsonPUtil.describe(valType) + " (" + val + ")");
        }
        return (JsonArrayExt)val;
    }

    private Located<String> getRequiredString(JsonObjectExt node, String prop, Supplier<String> reasonSupplier) {
        Located<String> optional = getOptionalString(node, prop);
        if (optional == null) {
            String reason = reasonSupplier == null ? "" : (" because " + reasonSupplier.get());
            throw new AvroSyntaxException("json object at " + node.getStartLocation() + " is expected to have a \""
                    + prop + "\" property" + reason);
        }
        return optional;
    }

    private Located<String> getOptionalString(JsonObjectExt node, String prop) {
        JsonValueExt val = node.get(prop);
        if (val == null) {
            return null;
        }
        JsonValue.ValueType valType = val.getValueType();
        if (valType != JsonValue.ValueType.STRING) {
            throw new AvroSyntaxException("\"" + prop + "\" property at " + val.getStartLocation()
                    + " is expected to be a string, not a " + JsonPUtil.describe(valType) + " (" + val + ")");
        }
        return new Located<>(((JsonStringExt)val).getString(), Util.convertLocation(val.getStartLocation()));
    }
}
