/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.parser.Located;
import com.linkedin.avroutil1.parser.exceptions.AvroSyntaxException;
import com.linkedin.avroutil1.parser.exceptions.JsonParseException;
import com.linkedin.avroutil1.parser.exceptions.ParseException;
import com.linkedin.avroutil1.parser.jsonpext.JsonObjectExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderWithLocations;
import com.linkedin.avroutil1.parser.jsonpext.JsonStringExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;
import com.linkedin.avroutil1.util.Util;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParsingException;

import java.io.StringReader;
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
     * @throws ParseException
     */
    private void parseSchemaDeclOrRef (
            JsonObjectExt node,
            AvscParseContext context,
            boolean topLevel
    ) throws ParseException {
        JsonValue.ValueType nodeType = node.getValueType();
        AvroSchema definedSchema;
        switch (nodeType) {
            case STRING: //primitive or ref
                throw new UnsupportedOperationException("TBD");
            case OBJECT: //record/enum/fixed/error
                Located<String> typeStr = getRequiredString(node, "type", () -> "it is a schema declaration");
                AvroType avroType = AvroType.fromJson(typeStr.getValue());
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
                    nameStr = getRequiredString(node, "name", () -> avroType + " is a named type");

                    namespaceStr = getOptionalString(node, "namespace");
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
                    docStr = getOptionalString(node, "doc");
                    if (docStr != null) {
                        doc = docStr.getValue();
                    }
                }

                switch (avroType) {
                    case RECORD:
                        //noinspection ConstantConditions
                        definedSchema = new AvroRecordSchema(
                                nameStr.getValue(), // != null
                                namespace,
                                doc);
                        //TODO - parse into fields
                        break;
                    case ENUM:
                        throw new UnsupportedOperationException("TBD");
                    case FIXED:
                        throw new UnsupportedOperationException("TBD");
                    default:
                        throw new IllegalStateException("unhandled: " + avroType);
                }
                //TODO - parse json props
                context.defineSchema(new Located<>(definedSchema, Util.convertLocation(node.getStartLocation())), topLevel);
                if (namespaceChanged) {
                    context.popNamespace();
                }
                return;
            case ARRAY:  //union
                throw new UnsupportedOperationException("TBD");
            default:
                throw new IllegalArgumentException("dont know how to parse a schema out ot " + nodeType + " at " + node.getStartLocation());
        }
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
                    + " is expected to be a string, not a " + valType + " (" + val + ")");
        }
        return new Located<>(((JsonStringExt)val).getString(), Util.convertLocation(val.getStartLocation()));
    }
}
