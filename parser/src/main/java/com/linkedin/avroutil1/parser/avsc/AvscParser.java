/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroArrayLiteral;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroBooleanLiteral;
import com.linkedin.avroutil1.model.AvroBytesLiteral;
import com.linkedin.avroutil1.model.AvroCollectionSchema;
import com.linkedin.avroutil1.model.AvroDoubleLiteral;
import com.linkedin.avroutil1.model.AvroEnumLiteral;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedLiteral;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroFloatLiteral;
import com.linkedin.avroutil1.model.AvroIntegerLiteral;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;
import com.linkedin.avroutil1.model.AvroLiteral;
import com.linkedin.avroutil1.model.AvroLogicalType;
import com.linkedin.avroutil1.model.AvroLongLiteral;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroNullLiteral;
import com.linkedin.avroutil1.model.AvroPrimitiveSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroStringLiteral;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.CodeLocation;
import com.linkedin.avroutil1.model.JsonPropertiesContainer;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final static BigInteger MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
    private final static BigInteger MIN_INT = BigInteger.valueOf(Integer.MIN_VALUE);
    private final static BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private final static BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    private final static BigDecimal MAX_FLOAT = BigDecimal.valueOf(Float.MAX_VALUE);
    private final static BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);

    /**
     * set of properties considered a part of the "core" avro specification for schemas.
     * any property that is NOT in this set is preserved as a property schema objects.
     */
    private final static Set<String> CORE_SCHEMA_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "aliases",
            "default", //vanilla treats this as core on enums only. we think that's confusing
            "doc",
            "fields",
            "items",
            "name",
            "namespace",
            "size",
            "symbols",
            "type",
            "values"
    )));

    /**
     * set of properties considered a part of the "core" avro specification for fields.
     * any property that is NOT in this set is preserved as a property field objects.
     */
    private final static Set<String> CORE_FIELD_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "aliases",
            "default",
            "doc",
            "name",
            "order",
            "type"
    )));

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
                return parseSimplePrimitiveOrRef((JsonStringExt) node, context);
            case OBJECT: //record/enum/fixed/array/map/error or a simpler type with extra props thrown-in
                return parseComplexSchema((JsonObjectExt) node, context, topLevel);
            case ARRAY:  //union
                return parseUnionSchema((JsonArrayExt) node, context, topLevel);
            default:
                throw new IllegalArgumentException("dont know how to parse a schema out of " + nodeType
                        + " at " + locationOf(context.getUri(), node));
        }
    }

    private SchemaOrRef parseSimplePrimitiveOrRef(
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
            //no logical type information, string representation or props in the schema if we got here
            return new SchemaOrRef(codeLocation, AvroPrimitiveSchema.forType(
                    codeLocation,
                    avroType,
                    null,
                    null,
                    0,
                    0,
                    JsonPropertiesContainer.EMPTY
            ));
        }
        //if we got here it means we found something like "record" as a type literal. which is not valid syntax
        throw new AvroSyntaxException("Illegal avro type \"" + typeString + "\" at " + codeLocation.getStart() + ". "
                + "Expecting a primitive type, an inline type definition or a reference the the fullname of a named type defined elsewhere");
    }

    private AvroPrimitiveSchema parseDecoratedPrimitiveSchema(
            JsonObjectExt primitiveNode,
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation,
            JsonPropertiesContainer props
    ) {
        AvroLogicalType logicalType = null;
        int scale = 0;
        int precision = 0;
        Parsed<AvroLogicalType> logicalTypeResult = parseLogicalType(primitiveNode, context, avroType, codeLocation);
        if (logicalTypeResult.hasIssues()) {
            context.addIssues(logicalTypeResult.getIssues());
        }
        logicalType = logicalTypeResult.getData(); //might be null
        Parsed<AvroJavaStringRepresentation> stringRepResult = parseStringRepresentation(primitiveNode, context, avroType, codeLocation);
        AvroJavaStringRepresentation stringRep = null;
        if (stringRepResult.hasIssues()) {
            context.addIssues(stringRepResult.getIssues());
        }
        if (AvroType.STRING.equals(avroType) && stringRepResult.hasData()) {
            stringRep = stringRepResult.getData();
        }
        Located<Integer> precisionResult = getOptionalInteger(primitiveNode, "precision", context);
        Located<Integer> scaleResult = getOptionalInteger(primitiveNode, "scale", context);
        boolean scaleAndPrecisionExpected = AvroType.BYTES.equals(avroType) && AvroLogicalType.DECIMAL.equals(logicalType);
        if (scaleAndPrecisionExpected) {
            boolean scaleAndPrecisionOK = true;
            int precisionValue = 0;
            int scaleValue = 0;
            //precision is actually required
            if (precisionResult == null) {
                context.addIssue(AvscIssues.precisionRequiredAndNotSet(codeLocation, avroType, logicalType));
                scaleAndPrecisionOK = false;
            }
            if (scaleAndPrecisionOK && scaleResult != null) {
                precisionValue = precisionResult.getValue();
                scaleValue = scaleResult.getValue();
                if (precisionValue < scaleValue) {
                    context.addIssue(AvscIssues.precisionSmallerThanScale(
                            locationOf(context.getUri(), precisionResult),
                            precisionValue,
                            locationOf(context.getUri(), scaleResult),
                            scaleValue
                    ));
                    scaleAndPrecisionOK = false;
                }
            }
            if (scaleAndPrecisionOK) {
                scale = scaleValue;
                precision = precisionValue;
            } else {
                //"cancel" the logicalType
                logicalType = null;
            }
        }

        return new AvroPrimitiveSchema(codeLocation, avroType, logicalType, stringRep, scale, precision, props);
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

        LinkedHashMap<String, JsonValueExt> propsMap = parseExtraProps(objectNode, CORE_SCHEMA_PROPERTIES);
        JsonPropertiesContainer props = propsMap.isEmpty() ? JsonPropertiesContainer.EMPTY : new JsonPropertiesContainerImpl(propsMap);

        AvroSchema definedSchema;
        if (avroType.isNamed()) {
            definedSchema = parseNamedSchema(objectNode, context, avroType, codeLocation, props);
        } else if (avroType.isCollection()) {
            definedSchema = parseCollectionSchema(objectNode, context, avroType, codeLocation, props);
        } else if (avroType.isPrimitive()) {
            definedSchema = parseDecoratedPrimitiveSchema(objectNode, context, avroType, codeLocation, props);
        } else {
            throw new IllegalStateException("unhandled avro type " + avroType + " at " + typeStr.getLocation());
        }

        context.defineSchema(new Located<>(definedSchema, codeLocation.getStart()), topLevel);

        return new SchemaOrRef(codeLocation, definedSchema);
    }

    private AvroNamedSchema parseNamedSchema(
            JsonObjectExt objectNode,
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation,
            JsonPropertiesContainer extraProps
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
                        doc,
                        extraProps
                );
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
                    AvroLiteral defaultValue = null;
                    if (fieldDefaultValueNode != null) {
                        if (fieldSchema.isResolved()) {
                            LiteralOrIssue defaultValurOrIssue = parseLiteral(fieldDefaultValueNode, fieldSchema.getSchema(), fieldName.getValue(), context);
                            if (defaultValurOrIssue.getIssue() == null) {
                                defaultValue = defaultValurOrIssue.getLiteral();
                            }
                            //TODO - handle issues
                        } else {
                            //we cant parse the default value yet since we dont have the schema to decode it with
                            //TODO - implement delayed default value parsing
                            throw new UnsupportedOperationException("delayed parsing of default value for " + fieldName.getValue() + " TBD");
                        }
                    }
                    LinkedHashMap<String, JsonValueExt> props = parseExtraProps(fieldDecl, CORE_FIELD_PROPERTIES);
                    JsonPropertiesContainer propsContainer = props.isEmpty() ? JsonPropertiesContainer.EMPTY : new JsonPropertiesContainerImpl(props);
                    AvroSchemaField field = new AvroSchemaField(fieldCodeLocation, fieldName.getValue(), null, fieldSchema, defaultValue, propsContainer);
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
                        context.addIssue(AvscIssues.badEnumDefaultValue(locationOf(context.getUri(), defaultStr),
                                defaultSymbol, schemaSimpleName, symbols));
                        //TODO - support "fixing" by selecting 1st symbol as default?
                        defaultSymbol = null;
                    }
                }
                namedSchema = new AvroEnumSchema(
                        codeLocation,
                        schemaSimpleName,
                        contextNamespace,
                        doc,
                        symbols,
                        defaultSymbol,
                        extraProps
                );
                break;
            case FIXED:
                JsonValueExt sizeNode = getRequiredNode(objectNode, "size", () -> "fixed types must have a size property");
                if (sizeNode.getValueType() != JsonValue.ValueType.NUMBER || !(((JsonNumberExt) sizeNode).isIntegral())) {
                    throw new AvroSyntaxException("size for fixed " + schemaSimpleName + " at "
                            + sizeNode.getStartLocation() + " expected to be an INTEGER, not a "
                            + JsonPUtil.describe(sizeNode.getValueType()) + " (" + sizeNode + ")");
                }
                int fixedSize = ((JsonNumberExt) sizeNode).intValue();
                Parsed<AvroLogicalType> logicalTypeResult = parseLogicalType(objectNode, context, avroType, codeLocation);
                if (logicalTypeResult.hasIssues()) {
                    context.addIssues(logicalTypeResult.getIssues());
                }
                namedSchema = new AvroFixedSchema(
                        codeLocation,
                        schemaSimpleName,
                        contextNamespace,
                        doc,
                        fixedSize,
                        logicalTypeResult.getData(),
                        extraProps
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
            CodeLocation codeLocation,
            JsonPropertiesContainer props
    ) {
        switch (avroType) {
            case ARRAY:
                JsonValueExt arrayItemsNode = getRequiredNode(objectNode, "items", () -> "array declarations must have an items property");
                SchemaOrRef arrayItemSchema = parseSchemaDeclOrRef(arrayItemsNode, context, false);
                return new AvroArraySchema(codeLocation, arrayItemSchema, props);
            case MAP:
                JsonValueExt mapValuesNode = getRequiredNode(objectNode, "values", () -> "map declarations must have a values property");
                SchemaOrRef mapValueSchema = parseSchemaDeclOrRef(mapValuesNode, context, false);
                return new AvroMapSchema(codeLocation, mapValueSchema, props);
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

    private LiteralOrIssue parseLiteral(
            JsonValueExt literalNode,
            AvroSchema schema,
            String fieldName,
            AvscParseContext context
    ) {
        AvroType avroType = schema.type();
        JsonValue.ValueType jsonType = literalNode.getValueType();
        AvscIssue issue;
        BigInteger bigIntegerValue;
        BigDecimal bigDecimalValue;
        byte[] bytes;
        switch (avroType) {
            case NULL:
                if (jsonType != JsonValue.ValueType.NULL) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroNullLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode)
                ));
            case BOOLEAN:
                if (jsonType != JsonValue.ValueType.FALSE && jsonType != JsonValue.ValueType.TRUE) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                boolean boolValue = jsonType == JsonValue.ValueType.TRUE;
                return new LiteralOrIssue(new AvroBooleanLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), boolValue
                ));
            case INT:
                if (jsonType != JsonValue.ValueType.NUMBER) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonNumberExt intNode = (JsonNumberExt) literalNode;
                if (!intNode.isIntegral()) {
                    //TODO - be more specific about this error (int vs float)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                bigIntegerValue = intNode.bigIntegerValue();
                if (bigIntegerValue.compareTo(MAX_INT) > 0 || bigIntegerValue.compareTo(MIN_INT) < 0) {
                    //TODO - be more specific about this error (out of signed int range)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroIntegerLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), bigIntegerValue.intValueExact()
                ));
            case LONG:
                if (jsonType != JsonValue.ValueType.NUMBER) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonNumberExt longNode = (JsonNumberExt) literalNode;
                if (!longNode.isIntegral()) {
                    //TODO - be more specific about this error (long vs float)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                bigIntegerValue = longNode.bigIntegerValue();
                if (bigIntegerValue.compareTo(MAX_LONG) > 0 || bigIntegerValue.compareTo(MIN_LONG) < 0) {
                    //TODO - be more specific about this error (out of signed long range)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroLongLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), bigIntegerValue.longValueExact()
                ));
            case FLOAT:
                if (jsonType != JsonValue.ValueType.NUMBER) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonNumberExt floatNode = (JsonNumberExt) literalNode;
                bigDecimalValue = floatNode.bigDecimalValue();
                if (bigDecimalValue.compareTo(MAX_FLOAT) > 0) {
                    //TODO - be more specific about this error (out of float range)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroFloatLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), bigDecimalValue.floatValue()
                ));
            case DOUBLE:
                if (jsonType != JsonValue.ValueType.NUMBER) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonNumberExt doubleNode = (JsonNumberExt) literalNode;
                bigDecimalValue = doubleNode.bigDecimalValue();
                if (bigDecimalValue.compareTo(MAX_DOUBLE) > 0) {
                    //TODO - be more specific about this error (out of double range)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroDoubleLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), bigDecimalValue.doubleValue()
                ));
            case BYTES:
                if (jsonType != JsonValue.ValueType.STRING) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonStringExt bytesNode = (JsonStringExt) literalNode;
                //spec says "strings, where Unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255"
                bytes = bytesNode.getString().getBytes(StandardCharsets.ISO_8859_1);
                return new LiteralOrIssue(new AvroBytesLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), bytes
                ));
            case FIXED:
                if (jsonType != JsonValue.ValueType.STRING) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                AvroFixedSchema fixedSchema = (AvroFixedSchema) schema;
                JsonStringExt fixedNode = (JsonStringExt) literalNode;
                //spec says "strings, where Unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255"
                bytes = fixedNode.getString().getBytes(StandardCharsets.ISO_8859_1);
                if (bytes.length != fixedSchema.getSize()) {
                    //TODO - be more specific about this error (wrong length)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroFixedLiteral(
                        fixedSchema, locationOf(context.getUri(), literalNode), bytes
                ));
            case STRING:
                if (jsonType != JsonValue.ValueType.STRING) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                JsonStringExt stringNode = (JsonStringExt) literalNode;
                return new LiteralOrIssue(new AvroStringLiteral(
                        (AvroPrimitiveSchema) schema, locationOf(context.getUri(), literalNode), stringNode.getString()
                ));
            case ENUM:
                if (jsonType != JsonValue.ValueType.STRING) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                AvroEnumSchema enumSchema = (AvroEnumSchema) schema;
                JsonStringExt enumNode = (JsonStringExt) literalNode;
                String enumValue = enumNode.getString();
                if (!enumSchema.getSymbols().contains(enumValue)) {
                    //TODO - be more specific about this error (unknown symbol)
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                return new LiteralOrIssue(new AvroEnumLiteral(
                        enumSchema, locationOf(context.getUri(), literalNode), enumValue
                ));
            case ARRAY:
                if (jsonType != JsonValue.ValueType.ARRAY) {
                    issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                            literalNode.toString(), avroType, fieldName);
                    context.addIssue(issue);
                    return new LiteralOrIssue(issue);
                }
                AvroArraySchema arraySchema = (AvroArraySchema) schema;
                AvroSchema valueSchema = arraySchema.getValueSchema();
                JsonArrayExt arrayNode = (JsonArrayExt) literalNode;
                ArrayList<AvroLiteral> values = new ArrayList<>(arrayNode.size());
                for (int i = 0; i < arrayNode.size(); i++) {
                    JsonValueExt valueNode = (JsonValueExt) arrayNode.get(i);
                    LiteralOrIssue value = parseLiteral(valueNode, valueSchema, fieldName, context);
                    //issues parsing any member value mean a failure to parse the array as a whole
                    if (value.getIssue() != null) {
                        //TODO - be more specific about this error (unparsable array element i)
                        //TODO - add "causedBy" to AvscIssue and use it here
                        issue = AvscIssues.badFieldDefaultValue(locationOf(context.getUri(), literalNode),
                                literalNode.toString(), avroType, fieldName);

                        context.addIssue(issue);
                        return new LiteralOrIssue(issue);
                    }
                    values.add(value.getLiteral());
                }
                return new LiteralOrIssue(new AvroArrayLiteral(
                        arraySchema, locationOf(context.getUri(), literalNode), values
                ));
            default:
                throw new UnsupportedOperationException("dont know how to parse a " + avroType + " at " + literalNode.getStartLocation()
                        + " out of a " + literalNode.getValueType() + " (" + literalNode + ")");
        }
    }

    private Parsed<AvroLogicalType> parseLogicalType(
            JsonObjectExt objectNode, //type declaration node
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation
    ) {
        Parsed<AvroLogicalType> result;
        Located<String> logicalTypeStr = getOptionalString(objectNode, "logicalType");
        AvroLogicalType logicalType;
        if (logicalTypeStr != null) {
            logicalType = AvroLogicalType.fromJson(logicalTypeStr.getValue());
            if (logicalType == null) {
                return new Parsed<>(AvscIssues.unknownLogicalType(
                        locationOf(context.getUri(), logicalTypeStr),
                        logicalTypeStr.getValue()
                ));
            } else {
                //we have a logicalType, see if it matches the type
                if (!logicalType.getParentTypes().contains(avroType)) {
                    return new Parsed<>(AvscIssues.mismatchedLogicalType(
                            locationOf(context.getUri(), logicalTypeStr),
                            logicalType,
                            avroType
                    ));
                }
                return new Parsed<>(logicalType);
            }
        } else {
            //no logical type (also no issues)
            return new Parsed<>((AvroLogicalType) null);
        }
    }

    private Parsed<AvroJavaStringRepresentation> parseStringRepresentation(
            JsonObjectExt objectNode, //type declaration node
            AvscParseContext context,
            AvroType avroType,
            CodeLocation codeLocation
    ) {
        Parsed<AvroJavaStringRepresentation> result = new Parsed<>();
        Located<String> repStr = getOptionalString(objectNode, "avro.java.string");
        AvroJavaStringRepresentation representation;
        if (repStr != null) {
            representation = AvroJavaStringRepresentation.fromJson(repStr.getValue());
            if (representation != null) {
                result.setData(representation);
            } else {
                result.recordIssue(AvscIssues.unknownJavaStringRepresentation(
                        locationOf(context.getUri(), repStr),
                        repStr.getValue()
                ));
            }
            //does string rep even make sense here? is type even a string?
            if (!AvroType.STRING.equals(avroType)) {
                result.recordIssue(AvscIssues.stringRepresentationOnNonString(
                        locationOf(context.getUri(), repStr),
                        repStr.getValue(),
                        avroType
                ));
            }
        }
        return result;
    }

    private LinkedHashMap<String, JsonValueExt> parseExtraProps(JsonObjectExt field, Set<String> toIgnore) {
        LinkedHashMap<String, JsonValueExt> results = new LinkedHashMap<>(3);
        for (Map.Entry<String, JsonValue> entry : field.entrySet()) { //doc says there are in the order they are in json
            String propName = entry.getKey();
            if (toIgnore.contains(propName)) {
                continue;
            }
            JsonValueExt propValue = (JsonValueExt) entry.getValue();
            results.put(propName, propValue);
        }
        return results;
    }

    private CodeLocation locationOf(URI uri, Located<?> something) {
        return new CodeLocation(uri, something.getLocation(), something.getLocation());
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

    private Located<Integer> getOptionalInteger(JsonObjectExt node, String prop, AvscParseContext context) {
        JsonValueExt val = node.get(prop);
        if (val == null) {
            return null;
        }
        JsonValue.ValueType valType = val.getValueType();
        if (valType != JsonValue.ValueType.NUMBER) {
            context.addIssue(AvscIssues.badPropertyType(
                    prop, locationOf(context.getUri(), val), val, valType.name(), JsonValue.ValueType.NUMBER.name()
                    ));
            return null;
        }
        JsonNumberExt numberNode = (JsonNumberExt) val;
        if (!numberNode.isIntegral()) {
            context.addIssue(AvscIssues.badPropertyType(
                    prop, locationOf(context.getUri(), val), val, "floating point value", "integer"
            ));
            return null;
        }
        return new Located<>(numberNode.intValueExact(), Util.convertLocation(val.getStartLocation()));
    }
}
