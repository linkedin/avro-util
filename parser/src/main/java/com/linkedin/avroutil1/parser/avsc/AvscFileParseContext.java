/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.exceptions.AvroSyntaxException;

import java.io.File;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * maintains state for parse operations in progress for a single avsc source
 * - completed (parsed) schemas, unresolved references, current namespace etc
 */
public class AvscFileParseContext {
    /**
     * represents the resource being parsed (typically an avsc file or a raw avsc string)
     */
    protected final URI uri;

    protected final AvscParser parser;
    /**
     * current (closest-defined) namespace. changes during parsing - pushed when a new
     * namespace is defined, popped when leaving the scope of a defined namespace.
     * initial value is [""]
     */
    protected final Deque<String> namespaceStack = new ArrayDeque<>();
    /**
     * all schemas defined (not simply referenced) within the current avsc being parsed.
     * the spec expects all nested schemas to be defined inline, so typical (large)
     * schemas may have 10s+ of nested record/enum/unions defined therein.
     * Schemas are only added to this collection once they are fully parsed.
     */
    protected final List<AvroSchema> definedSchemas = new ArrayList<>();
    /**
     * schemas out of the above collection that have a name, by their full name
     */
    protected final Map<String, AvroNamedSchema> definedNamedSchemas = new HashMap<>();
    /**
     * the top level (root, outer-most) schema in the avsc being parsed.
     * avsc sources typically have a single top level schema, though it may be
     * a primitive or a union/collection
     */
    protected AvroSchema topLevelSchema = null;
    /**
     * any issues encountered during parsing
     */
    protected List<AvscIssue> issues = new ArrayList<>();
    /**
     * references in this avsc that are not in this avsc (to be resolved by a wider context)
     */
    protected HashSet<SchemaOrRef> externalReferences = new HashSet<>();
    /**
     * fields (of records) in this avsc who's default value cannot be parsed because their schema
     * is not in this avsc (and should be parsed after the schema is resolved)
     */
    protected HashSet<AvroSchemaField> fieldsWithUnparsedDefaults = new HashSet<>();

    public AvscFileParseContext(String avsc, AvscParser parser) {
        try {
            this.uri = new URI("avsc://" + avsc.hashCode());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        this.parser = parser;
        initializeNamespace();
    }

    public AvscFileParseContext(File avsc, AvscParser parser) {
        this.uri = avsc.toURI();
        this.parser = parser;
        initializeNamespace();
    }

    protected void initializeNamespace() {
        if (!namespaceStack.isEmpty()) {
            throw new IllegalStateException("this is a bug");
        }
        namespaceStack.push("");
    }

    public URI getUri() {
        return uri;
    }

    public AvscParser getParser() {
        return parser;
    }

    public String getCurrentNamespace() {
        return namespaceStack.peek();
    }

    public void pushNamespace(String newNamespace) {
        // empty namespace ("") refers to the global namespace.
        if (newNamespace == null) {
            throw new IllegalArgumentException("new namespace cannot be null");
        }
        if (newNamespace.equals(getCurrentNamespace())) {
            throw new IllegalArgumentException("new namespace " + newNamespace + " same as current namespace");
        }
        namespaceStack.push(newNamespace);
    }

    public void popNamespace() {
        if (namespaceStack.isEmpty()) {
            throw new IllegalStateException("this is a bug");
        }
        namespaceStack.pop();
    }

    /**
     * defines a new (completely parsed) schema
     * @param schema new schema
     * @param isTopLevel true if schema is the top-level schema of the avsc being parsed
     */
    public void defineSchema(AvroSchema schema, boolean isTopLevel) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (isTopLevel) {
            if (topLevelSchema != null) {
                throw new IllegalStateException("cannot set " + schema + " as top level schema "
                        + "for context as thats already set to " + topLevelSchema);
            }
            topLevelSchema = schema;
        }
        definedSchemas.add(schema);
        if (schema.type().isNamed()) {
            AvroNamedSchema namedSchema = (AvroNamedSchema) schema;
            AvroSchema other = definedNamedSchemas.putIfAbsent(namedSchema.getFullName(), namedSchema);
            if (other != null) {
                throw new AvroSyntaxException(schema + " defined in " + schema.getCodeLocation() + " conflicts with " + other);
            }
        }
    }

    public void addIssue(AvscIssue issue) {
        if (issue == null) {
            throw new IllegalArgumentException("issue cannot be null");
        }
        issues.add(issue);
    }

    public void addIssues(Collection<AvscIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues cannot be null or empty");
        }
        this.issues.addAll(issues);
    }

    /**
     * iterates over all defined schemas in this file and tries to resolve references that could
     * not be resolved during parsing - for example self-references in schemas
     */
    public void resolveReferences() {
        for (AvroSchema schema : definedSchemas) {
            resolveReferences(schema);
        }

        // Once all schemas are resolved for this file, we can process the defaults for fields
        for (AvroSchema schema : definedSchemas) {
            processAllDefaults(schema);
        }
    }

    private void processAllDefaults(AvroSchema schema) {
        AvroType type = schema.type();
        if (type == AvroType.RECORD) {
            AvroRecordSchema recordSchema = (AvroRecordSchema) schema;
            List<AvroSchemaField> fields = recordSchema.getFields();
            for (AvroSchemaField field : fields) {
                // If field has no default value, we can skip it
                if (!field.hasDefaultValue()) {
                    continue;
                }
                SchemaOrRef fieldSchema = field.getSchemaOrRef();
                // If schema is not fully defined, add it to the list of fields with unparsed defaults
                if (!fieldSchema.isFullyDefined()) {
                    fieldsWithUnparsedDefaults.add(field);
                    continue;

                // schema is defined and default value is unparsed.
                } else if (field.getDefaultValue() instanceof AvscUnparsedLiteral) {
                    AvscParseUtil.lateParseFieldDefault(field, this);
                }
            }
        }
    }

    private void resolveReferences(AvroSchema schema) {
        AvroType type = schema.type();
        switch (type) {
            case RECORD:
                AvroRecordSchema recordSchema = (AvroRecordSchema) schema;
                List<AvroSchemaField> fields = recordSchema.getFields();
                for (AvroSchemaField field : fields) {
                    SchemaOrRef fieldSchema = field.getSchemaOrRef();
                    boolean wasDefinedBefore = fieldSchema.isFullyDefined();
                    if (!wasDefinedBefore) {
                        resolveReferences(fieldSchema);
                    }
                }
                break;
            case UNION:
                AvroUnionSchema unionSchema = (AvroUnionSchema) schema;
                List<SchemaOrRef> types = unionSchema.getTypes();
                for (SchemaOrRef unionType : types) {
                    resolveReferences(unionType);
                }
                break;
            case ARRAY:
                AvroArraySchema arraySchema = (AvroArraySchema) schema;
                SchemaOrRef arrayValuesType = arraySchema.getValueSchemaOrRef();
                resolveReferences(arrayValuesType);
                break;
            case MAP:
                AvroMapSchema mapSchema = (AvroMapSchema) schema;
                SchemaOrRef mapValuesType = mapSchema.getValueSchemaOrRef();
                resolveReferences(mapValuesType);
                break;
            default:
                break;
        }
    }

    private void resolveReferences(SchemaOrRef possiblyRef) {
        if (possiblyRef.isResolved()) {
            //either an already- resolved reference or an inline definition
            if (possiblyRef.getDecl() != null) {
                //recurse into inline definitions
                resolveReferences(possiblyRef.getDecl());
            }
        } else {
            //unresolved (and so must be a) reference

            String simpleName = possiblyRef.getRef();
            AvroSchema simpleNameResolution = definedNamedSchemas.get(simpleName);
            AvroSchema inheritedNameResolution = null;

            String inheritedName = possiblyRef.getInheritedName();
            if (inheritedName != null) {
                inheritedNameResolution = definedNamedSchemas.get(inheritedName);
            }

            if (inheritedNameResolution != null) {
                possiblyRef.setResolvedTo(inheritedNameResolution);
                if (simpleNameResolution != null) {
                    String msg = "Two different schemas found for reference " + simpleName + " with inherited name "
                        + inheritedName + ". Only one should exist.";
                    AvscIssue issue = new AvscIssue(possiblyRef.getCodeLocation(), IssueSeverity.WARNING, msg,
                        new IllegalStateException(msg));
                    issues.add(issue);
                }
            } else if (simpleNameResolution != null) {
                possiblyRef.setResolvedTo(simpleNameResolution);
            } else {
                externalReferences.add(possiblyRef);
            }
        }
    }

    public AvroSchema getTopLevelSchema() {
        return topLevelSchema;
    }

    public List<AvroSchema> getAllDefinedSchemas() {
        return definedSchemas;
    }

    public Map<String, AvroNamedSchema> getDefinedNamedSchemas() {
        return definedNamedSchemas;
    }

    public List<AvscIssue> getIssues() {
        return issues;
    }

    public Set<SchemaOrRef> getExternalReferences() {
        return externalReferences;
    }

    public Set<AvroSchemaField> getFieldsWithUnparsedDefaults() {
        return fieldsWithUnparsedDefaults;
    }
}
