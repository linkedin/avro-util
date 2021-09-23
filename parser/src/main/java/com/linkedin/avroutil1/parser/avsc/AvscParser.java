/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroCollectionSchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroNamedSchema;
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
import java.net.URI;
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
            Throwable rootCause = Util.rootCause(e);
            String message = rootCause.getMessage();
            if (message != null && message.startsWith("Unexpected char 47")) {
                //47 is ascii for '/' (utf-8 matches ascii on "low" characters). we are going to assume this means
                //someone tried putting a //comment into an avsc source
                result.recordError(new JsonParseException("comments not supported in json at" + e.getLocation(), e));
            } else {
                result.recordError(new JsonParseException("json parse error at " + e.getLocation(), e));
            }
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
        switch (nodeType) {
            case STRING: //primitive or ref
                return parsePrimitiveOrRef((JsonStringExt) node, context);
            case OBJECT: //record/enum/fixed/array/map/error
                return parseComplexSchema((JsonObjectExt) node, context, topLevel);
            case ARRAY:  //union
                return parseUnionSchema((JsonArrayExt) node, context, topLevel);
            default:
                throw new IllegalArgumentException("dont know how to parse a schema out of " + nodeType
                        + " at " + locationOf(context.getUri(), node));
        }
    }

    private SchemaOrRef parsePrimitiveOrRef(
            JsonStringExt stringNode,
            AvscParseContext context
    ) {
        CodeLocation codeLocation = locationOf(context.getUri(), stringNode);
        String typeString = stringNode.getString();
        AvroType avroType = AvroType.fromJson(typeString);
        //TODO - screen for reserved words??
        if (avroType == null) {
            //assume it's a ref
            return new SchemaOrRef(codeLocation, typeString);
        }
        if (avroType.isPrimitive()) {
            return new SchemaOrRef(codeLocation, AvroPrimitiveSchema.forType(codeLocation, avroType));
        }
        //if we got here it means we found something like "record" as a type literal. which is not valid syntax
        throw new AvroSyntaxException("Illegal avro type \"" + typeString + "\" at " + codeLocation.getStart() + ". "
                + "Expecting a primitive type, an inline type definition or a reference the the fullname of a named type defined elsewhere");
    }

    private SchemaOrRef parseComplexSchema(
            JsonObjectExt objectNode,
            AvscParseContext context,
            boolean topLevel
    ) {
        CodeLocation codeLocation = locationOf(context.getUri(), objectNode);
        Located<String> typeStr = getRequiredString(objectNode, "type", () -> "it is a schema declaration");
        AvroType avroType = AvroType.fromJson(typeStr.getValue());
        if (avroType == null) {
            throw new AvroSyntaxException("unknown avro type \"" + typeStr.getValue() + "\" at "
                    + typeStr.getLocation() + ". expecting \"record\", \"enum\" or \"fixed\"");
        }

        AvroSchema definedSchema;
        if (avroType.isNamed()) {
            definedSchema = parseNamedSchema(objectNode, context, avroType, codeLocation);
        } else if (avroType.isCollection()) {
            definedSchema = parseCollectionSchema(objectNode, context, avroType, codeLocation);
        } else {
            throw new IllegalStateException("unhandled avro type " + avroType + " at " + typeStr.getLocation());
        }

        //TODO - parse json props

        context.defineSchema(new Located<>(definedSchema, codeLocation.getStart()), topLevel);

        return new SchemaOrRef(codeLocation, definedSchema);
    }

    private AvroNamedSchema parseNamedSchema(
            JsonObjectExt objectNode,
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation
    ) {
        Located<String> nameStr = getRequiredString(objectNode, "name", () -> avroType + " is a named type");
        Located<String> namespaceStr = getOptionalString(objectNode, "namespace");
        //technically the avro spec does not allow "doc" on type fixed, but screw that
        Located<String> docStr = getOptionalString(objectNode, "doc");

        String name = nameStr.getValue();
        String namespace = namespaceStr != null ? namespaceStr.getValue() : null;
        String doc = docStr != null ? docStr.getValue() : null;

        String schemaSimpleName;
        String schemaNamespace;
        if (name.contains(".")) {
            //the name specified is a full name (namespace included)
            context.addIssue(AvscIssues.useOfFullName(
                    new CodeLocation(context.getUri(), nameStr.getLocation(), nameStr.getLocation()),
                    avroType, name));
            if (namespace != null) {
                //namespace will be ignored, but it's confusing to even list it
                context.addIssue(AvscIssues.ignoredNamespace(
                        new CodeLocation(context.getUri(), namespaceStr.getLocation(), namespaceStr.getLocation()),
                        avroType, namespace, name));
            }
            //TODO - validate names (no ending in dot, no spaces, etc)
            int lastDot = name.lastIndexOf('.');
            schemaSimpleName = name.substring(lastDot + 1);
            schemaNamespace = name.substring(0, lastDot);
        } else {
            schemaSimpleName = name;
            schemaNamespace = namespace;
        }

        String contextNamespace = context.getCurrentNamespace(); // != null
        boolean namespaceChanged = false;
        //check if context namespace changed
        if (schemaNamespace != null) {
            if (!contextNamespace.equals(schemaNamespace)) {
                context.pushNamespace(schemaNamespace);
                namespaceChanged = true;
                contextNamespace = schemaNamespace;
            }
        }

        AvroNamedSchema namedSchema;

        switch (avroType) {
            case RECORD:
                AvroRecordSchema recordSchema = new AvroRecordSchema(
                        codeLocation,
                        schemaSimpleName,
                        contextNamespace,
                        doc);
                JsonArrayExt fieldsNode = getRequiredArray(objectNode, "fields", () -> "all avro records must have fields");
                List<AvroSchemaField> fields = new ArrayList<>(fieldsNode.size());
                for (int fieldNum = 0; fieldNum < fieldsNode.size(); fieldNum++) {
                    JsonValueExt fieldDeclNode = (JsonValueExt) fieldsNode.get(fieldNum); //!=null
                    JsonValue.ValueType fieldNodeType = fieldDeclNode.getValueType();
                    if (fieldNodeType != JsonValue.ValueType.OBJECT) {
                        throw new AvroSyntaxException("field " + fieldNum + " for record " + schemaSimpleName + " at "
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
                namedSchema = recordSchema;
                break;
            case ENUM:
                JsonArrayExt symbolsNode = getRequiredArray(objectNode, "symbols", () -> "all avro enums must have symbols");
                List<String> symbols = new ArrayList<>(symbolsNode.size());
                for (int ordinal = 0; ordinal < symbolsNode.size(); ordinal++) {
                    JsonValueExt symbolNode = (JsonValueExt) symbolsNode.get(ordinal);
                    JsonValue.ValueType symbolNodeType = symbolNode.getValueType();
                    if (symbolNodeType != JsonValue.ValueType.STRING) {
                        throw new AvroSyntaxException("symbol " + ordinal + " for enum " + schemaSimpleName + " at "
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
                        throw new AvroSyntaxException("enum " + schemaSimpleName + " has a default value of " + defaultSymbol
                                + " at " + defaultStr.getLocation() + " which is not in its list of symbols (" + symbols + ")");
                    }
                }
                namedSchema = new AvroEnumSchema(
                        codeLocation,
                        schemaSimpleName,
                        contextNamespace,
                        doc,
                        symbols,
                        defaultSymbol);
                break;
            case FIXED:
                JsonValueExt sizeNode = getRequiredNode(objectNode, "size", () -> "fixed types must have a size property");
                if (sizeNode.getValueType() != JsonValue.ValueType.NUMBER || !(((JsonNumberExt) sizeNode).isIntegral())) {
                    throw new AvroSyntaxException("size for fixed " + schemaSimpleName + " at "
                            + sizeNode.getStartLocation() + " expected to be an INTEGER, not a "
                            + JsonPUtil.describe(sizeNode.getValueType()) + " (" + sizeNode + ")");
                }
                int fixedSize = ((JsonNumberExt) sizeNode).intValue();
                namedSchema = new AvroFixedSchema(
                        codeLocation,
                        schemaSimpleName,
                        contextNamespace,
                        doc,
                        fixedSize
                );
                break;
            default:
                throw new IllegalStateException("unhandled: " + avroType + " for object at " + codeLocation.getStart());
        }

        if (namespaceChanged) {
            context.popNamespace();
        }
        return namedSchema;
    }

    private AvroCollectionSchema parseCollectionSchema(
            JsonObjectExt objectNode,
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation
    ) {
        switch (avroType) {
            case ARRAY:
                JsonValueExt arrayItemsNode = getRequiredNode(objectNode, "items", () -> "array declarations must have an items property");
                SchemaOrRef arrayItemSchema = parseSchemaDeclOrRef(arrayItemsNode, context, false);
                return new AvroArraySchema(codeLocation, arrayItemSchema);
            case MAP:
                JsonValueExt mapValuesNode = getRequiredNode(objectNode, "values", () -> "map declarations must have a values property");
                SchemaOrRef mapValueSchema = parseSchemaDeclOrRef(mapValuesNode, context, false);
                return new AvroMapSchema(codeLocation, mapValueSchema);
            default:
                throw new IllegalStateException("unhandled: " + avroType + " for object at " + codeLocation.getStart());
        }
    }

    private SchemaOrRef parseUnionSchema(
            JsonArrayExt arrayNode,
            AvscParseContext context,
            boolean topLevel
    ) {
        CodeLocation codeLocation = locationOf(context.getUri(), arrayNode);
        List<SchemaOrRef> unionTypes = new ArrayList<>(arrayNode.size());
        for (JsonValue jsonValue : arrayNode) {
            JsonValueExt unionNode = (JsonValueExt) jsonValue;
            SchemaOrRef type = parseSchemaDeclOrRef(unionNode, context, false);
            unionTypes.add(type);
        }
        AvroUnionSchema unionSchema = new AvroUnionSchema(codeLocation);
        unionSchema.setTypes(unionTypes);
        context.defineSchema(new Located<>(unionSchema, codeLocation.getStart()), topLevel);
        return new SchemaOrRef(codeLocation, unionSchema);
    }

    private CodeLocation locationOf(URI uri, JsonValueExt node) {
        TextLocation startLocation = Util.convertLocation(node.getStartLocation());
        TextLocation endLocation = Util.convertLocation(node.getEndLocation());
        return new CodeLocation(uri, startLocation, endLocation);
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
