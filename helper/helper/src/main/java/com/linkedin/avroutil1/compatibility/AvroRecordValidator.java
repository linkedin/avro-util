/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.avropath.ArrayPositionPredicate;
import com.linkedin.avroutil1.compatibility.avropath.AvroPath;
import com.linkedin.avroutil1.compatibility.avropath.LocationStep;
import com.linkedin.avroutil1.compatibility.avropath.MapKeyPredicate;
import com.linkedin.avroutil1.compatibility.avropath.PathElement;
import com.linkedin.avroutil1.compatibility.avropath.UnionTypePredicate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * utility class to verify that a given {@link org.apache.avro.generic.IndexedRecord} "valid": <br>
 * <ul>
 *     <li>
 *         all fields must have value that matches their declared schema (specifically this means all fields that
 *         do not allow null values must be populated)
 *     </li>
 *     <li>
 *         the graph of objects must be cycle-free
 *     </li>
 *     <li>
 *         the same string type (either {@link org.apache.avro.util.Utf8} of {@link java.lang.String} must
 *         be used for all String values in the entire data graph
 *     </li>
 * </ul>
 */
public class AvroRecordValidator {

    public static List<AvroRecordValidationIssue> validate(IndexedRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        Schema schema = record.getSchema();
        if (schema == null) {
            throw new IllegalStateException("no schema on record " + record);
        }

        Context context = new Context(record);
        traverse(schema, record, context);
        return context.issues;
    }

    private static void traverse(Schema schema, Object instance, Context context) {
        Schema.Type schemaType = schema.getType();
        String schemaFullname;
        Schema instanceDeclaredSchema;
        switch (schemaType) {
            //simple primitives
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
                if (!isInstanceOf(instance, schema)) {
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                }
                break;
            //strings
            case STRING:
                if (!(instance instanceof CharSequence)) { //catches nulls
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                //check for consistent use of the same string type in an object graph
                Map<Class<? extends CharSequence>, AvroPath> typesSeen = context.stringTypesSeen();
                if (context.recordStringType((CharSequence) instance) && typesSeen.size() > 1) {
                    StringJoiner csv = new StringJoiner(", ");
                    typesSeen.forEach((type, path) -> {
                        csv.add(path + " is " + type.getName());
                    });
                    AvroRecordValidationIssue issue = new AvroRecordValidationIssue("use of multiple string types: " + csv, Severity.WARNING);
                    context.recordIssue(issue);
                }
                break;
            //named types
            case ENUM:
                if (!(instance instanceof Enum || instance instanceof GenericEnumSymbol)) {
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                schemaFullname = schema.getFullName();
                instanceDeclaredSchema = AvroSchemaUtil.getDeclaredSchema(instance); //null under ancient avro :-(
                if (instanceDeclaredSchema != null && !schemaFullname.equals(instanceDeclaredSchema.getFullName())) {
                    context.recordIssue(new AvroRecordValidationIssue("value should be a(n) " + schemaFullname + " but instead is "
                            + instanceDeclaredSchema.getFullName()));
                    break;
                }
                if (context.isSpecificRecord && !(instance instanceof Enum)) {
                    context.recordIssue(new AvroRecordValidationIssue(
                            "(generic) EnumSymbol " + schemaFullname + " but root object is SpecificRecord",
                            Severity.WARNING
                    ));
                } else if (!context.isSpecificRecord && (instance instanceof Enum)) {
                    context.recordIssue(new AvroRecordValidationIssue(
                            "(specific) Enum " + schemaFullname + " but root object is GenericRecord",
                            Severity.WARNING
                    ));
                }
                break;
            case FIXED:
                if (!(instance instanceof GenericFixed)) { //also covers Specific Fixed classes under all avro versions
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                schemaFullname = schema.getFullName();
                instanceDeclaredSchema = AvroSchemaUtil.getDeclaredSchema(instance); //null under ancient avro :-(
                if (instanceDeclaredSchema != null && !schemaFullname.equals(instanceDeclaredSchema.getFullName())) {
                    context.recordIssue(new AvroRecordValidationIssue("value should be a(n) " + schemaFullname + " but instead is "
                            + instanceDeclaredSchema.getFullName()));
                    break;
                }
                //TODO - validate fixed length matches
                if (context.isSpecificRecord && !(instance instanceof SpecificFixed)) {
                    context.recordIssue(new AvroRecordValidationIssue(
                            "(generic) Fixed " + schemaFullname + " but root object is SpecificRecord",
                            Severity.WARNING
                    ));
                } else if (!context.isSpecificRecord && (instance instanceof SpecificFixed)) {
                    context.recordIssue(new AvroRecordValidationIssue(
                            "(specific) Fixed " + schemaFullname + " but root object is GenericRecord",
                            Severity.WARNING
                    ));
                }
                break;
            case RECORD:
                if (!(instance instanceof IndexedRecord)) { //also covers Specific classes under all avro versions
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                IndexedRecord record = (IndexedRecord) instance;

                //records are the only thing that can create loops in a graph
                if (context.markRecordSeen(record)) {
                    context.recordIssue(new AvroRecordValidationIssue("loop in data graph"));
                    break; //no need to pop this record, as our caller(s) will.
                }
                try {
                    schemaFullname = schema.getFullName();
                    instanceDeclaredSchema = AvroSchemaUtil.getDeclaredSchema(instance); //!= null
                    //noinspection ConstantConditions
                    if (!schemaFullname.equals(instanceDeclaredSchema.getFullName())) {
                        context.recordIssue(new AvroRecordValidationIssue("value should be a(n) " + schemaFullname + " but instead is "
                                + instanceDeclaredSchema.getFullName()));
                        break;
                    }
                    if (context.isSpecificRecord && !(instance instanceof SpecificRecord)) {
                        context.recordIssue(new AvroRecordValidationIssue(
                                "GenericRecord " + schemaFullname + " but root object is SpecificRecord",
                                Severity.WARNING
                        ));
                    } else if (!context.isSpecificRecord && (instance instanceof SpecificRecord)) {
                        context.recordIssue(new AvroRecordValidationIssue(
                                "SpecificRecord " + schemaFullname + " but root object is GenericRecord",
                                Severity.WARNING
                        ));
                    }
                    List<Schema.Field> schemaFields = schema.getFields();
                    List<Schema.Field> instanceFields = instanceDeclaredSchema.getFields();

                    int i = 0;
                    for (Schema.Field field : schemaFields) {
                        String fieldName = field.name();
                        Schema.Field instanceField;
                        try {
                            instanceField = instanceFields.get(i);
                        } catch (IndexOutOfBoundsException e) {
                            context.recordIssue(new AvroRecordValidationIssue("expected field " + fieldName + " not found on record"));
                            break; //no more fields to validate
                        }
                        if (!fieldName.equals(instanceField.name())) {
                            context.recordIssue(new AvroRecordValidationIssue("expected field at position " + field.pos() + " to be "
                                    + fieldName + " but record has field " + instanceField.name()));
                            continue; //move to next field
                        }
                        //field positions match because thats the order they are listed in
                        context.appendPath(new LocationStep(".", fieldName));
                        try {
                            Schema fieldSchema = field.schema();
                            Object fieldValue = record.get(field.pos());
                            traverse(fieldSchema, fieldValue, context);
                        } finally {
                            context.popPath();
                        }
                        i++;
                    }
                    break;
                } finally {
                    context.popRecordSeen(record);
                }
            //collections
            case ARRAY:
                if (!(instance instanceof List)) {
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                List<?> values = (List<?>) instance;
                int numValues = values.size();
                Schema elementSchema = schema.getElementType();
                for (int j = 0; j < numValues; j++) {
                    context.appendPath(new ArrayPositionPredicate(j));
                    try {
                        Object value = values.get(j);
                        traverse(elementSchema, value, context);
                    } finally {
                        context.popPath();
                    }
                }
                break;
            case MAP:
                if (!(instance instanceof Map)) {
                    context.recordIssue(new AvroRecordValidationIssue("type should be a(n) " + schemaType + " but instead is " + describe(instance)));
                    break;
                }
                Map<?, ?> map = (Map<?, ?>) instance;
                Set<? extends Map.Entry<?, ?>> entries = map.entrySet();
                Schema valueSchema = schema.getValueType();
                for (Map.Entry<?, ?> entry : entries) {
                    Object key = entry.getKey();
                    if (key == null) {
                        context.recordIssue(new AvroRecordValidationIssue("map contains null key"));
                        continue;
                    }
                    if (!(key instanceof CharSequence)) {
                        context.recordIssue(new AvroRecordValidationIssue("map keys should be strings but found " + describe(key)));
                        continue;
                    }
                    //TODO - check key string type?
                    String strKey = String.valueOf(key);
                    context.appendPath(new MapKeyPredicate(strKey));
                    try {
                        Object value = entry.getValue();
                        traverse(valueSchema, value, context);
                    } finally {
                        context.popPath();
                    }
                }
                break;
            //unions
            case UNION:
                List<Schema> branches = schema.getTypes();
                List<Schema> matches = new ArrayList<>(1);
                for (int k = 0; k < branches.size(); k++) {
                    Schema branch = branches.get(k);
                    if (isInstanceOf(instance, branch)) {
                        matches.add(branch);
                    }
                }
                if (matches.isEmpty()) {
                    context.recordIssue(new AvroRecordValidationIssue("value " + describe(instance) + " does not match any union branch in " + branches));
                    break;
                }
                if (matches.size() > 1) {
                    context.recordIssue(new AvroRecordValidationIssue("value " + describe(instance) + " matches multiple union branches: " + matches));
                    break;
                }
                Schema branch = matches.get(0);
                context.appendPath(new UnionTypePredicate(branch.getName()));
                try {
                    traverse(branch, instance, context);
                } finally {
                    context.popPath();
                }
                break;
            default:
                throw new IllegalStateException("unhandled " + schemaType);
        }
    }

    private static boolean isInstanceOf(Object instance, Schema schema) {
        Schema.Type schemaType = schema.getType();
        switch (schemaType) {
            //primitives
            case NULL:
                return instance == null;
            case BOOLEAN:
                return instance instanceof Boolean;
            case INT:
                return instance instanceof Integer;
            case LONG:
                return instance instanceof Long;
            case FLOAT:
                return instance instanceof Float;
            case DOUBLE:
                return instance instanceof Double;
            case BYTES:
                return instance instanceof ByteBuffer;
            case STRING:
                return instance instanceof CharSequence;
            //named types
            case ENUM:
                return instance instanceof Enum || instance instanceof GenericEnumSymbol;
            case FIXED:
                return instance instanceof GenericFixed;
            case RECORD:
                return instance instanceof IndexedRecord;
            //collections
            case MAP:
                return instance instanceof Map;
            case ARRAY:
                return instance instanceof List;
            //no such thing as instanceof union
            default:
                throw new IllegalStateException("unhandled " + schemaType);
        }
    }

    private static class Context {
        private final IndexedRecord root;
        private final boolean isSpecificRecord;
        private final AvroPath avroPath = new AvroPath();
        private final List<AvroRecordValidationIssue> issues = new ArrayList<>(1);
        //used to detect loops in the object graph
        private final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        private final Map<Class<? extends CharSequence>, AvroPath> stringTypesSeen = new HashMap<>(1);

        public Context(IndexedRecord root) {
            if (root == null) {
                throw new IllegalArgumentException("root record required");
            }
            this.root = root;
            this.isSpecificRecord = AvroCompatibilityHelper.isSpecificRecord(root);
            //traversal will populate root into recordsSeen
        }

        public void appendPath(PathElement element) {
            avroPath.appendPath(element);
        }

        public void popPath() {
            avroPath.pop();
        }

        /**
         * @param stringInstance a string instance. not null.
         * @return true if this string type is new in this graph
         */
        public boolean recordStringType(CharSequence stringInstance) {
            Class<? extends CharSequence> stringType = stringInstance.getClass();
            if (stringTypesSeen.containsKey(stringType)) {
                return false;
            }
            stringTypesSeen.put(stringType, avroPath.copy());
            return true;
        }

        public Map<Class<? extends CharSequence>, AvroPath> stringTypesSeen() {
            return stringTypesSeen;
        }

        public boolean markRecordSeen(IndexedRecord record) {
            if (seen.containsKey(record)) {
                return true;
            }
            seen.put(record, Boolean.TRUE);
            return false;
        }

        public void popRecordSeen(IndexedRecord record) {
            if (seen.remove(record) == null) {
                throw new IllegalStateException("record popped but never seen");
            }
        }

        public void recordIssue(AvroRecordValidationIssue issue) {
            if (issue.getPath() == null) {
                issue.setPath(avroPath.copy());
            }
            issues.add(issue);
        }
    }

    private static String describe(Object obj) {
        if (obj == null) {
            return "null";
        }
        //TODO - max length
        return obj + " (a " + obj.getClass().getName() + ")";
    }
}
