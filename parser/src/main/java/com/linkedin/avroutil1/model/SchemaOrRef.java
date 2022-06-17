/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import com.linkedin.avroutil1.parser.exceptions.UnresolvedReferenceException;

/**
 * represents either a declared avro schema (which may or may not be a named type)
 * or a reference to an avro schema (by FQCN, meaning the schema referenced is named)
 */
public class SchemaOrRef implements LocatedCode {
    private final CodeLocation codeLocation;
    /**
     * a declared schema, if this represents a declared schema
     */
    private final AvroSchema decl;
    /**
     * the FQCN of a referenced schema, if this represents a reference
     */
    private final String ref;
    /**
     * the namespace of the parent schema, which might be necessary for reference resolution if the ref is not a fullname
     */
    private final String parentNamespace;
    /**
     * the schema (either declared or being referenced). in case of references
     * this may be set at a later phase of parsing (or may never be resolved).
     * once set this cannot change.
     */
    private AvroSchema schema;

    public SchemaOrRef(CodeLocation codeLocation, AvroSchema decl) {
        if (decl == null) {
            throw new IllegalArgumentException("schema definition cannot be null");
        }
        this.codeLocation = codeLocation;
        this.decl = decl;
        this.ref = null;
        this.parentNamespace = null;
        this.schema = decl;
    }

    public SchemaOrRef(CodeLocation codeLocation, String ref, String parentNamespace) {
        if (ref == null) {
            throw new IllegalArgumentException("schema reference cannot be null");
        }
        this.codeLocation = codeLocation;
        this.decl = null;
        this.ref = ref;
        this.parentNamespace = parentNamespace;
        this.schema = null;
    }

    @Override
    public CodeLocation getCodeLocation() {
        return codeLocation;
    }

    public AvroSchema getDecl() {
        return decl;
    }

    public String getRef() {
        return ref;
    }

    // null if ref already is a FQN or if there is no parent namespace.
    public String getInheritedName() {
        // parent namespace can be empty (refers to the null namespace).
        boolean parentNamespaceIsNonEmpty = !(this.parentNamespace == null);
        boolean refHasNoNamespace = this.ref != null && !this.ref.contains(".");
        if (refHasNoNamespace && parentNamespaceIsNonEmpty) {
            return this.parentNamespace + "." + this.ref;
        }
        return null;
    }

    public String getParentNamespace() {
        return parentNamespace;
    }

    /**
     * @return true if this is declared inline or a resolved reference
     */
    public boolean isResolved() {
        return schema != null;
    }

    public AvroSchema getSchema() {
        if (!isResolved()) {
            throw new UnresolvedReferenceException("unresolved ref to " + ref);
        }
        return schema;
    }

    public void setResolvedTo(AvroSchema referencedSchema) {
        if (referencedSchema == null) {
            throw new IllegalArgumentException("referencedSchema cannot be null");
        }
        if (ref == null) {
            throw new IllegalStateException("this schema (" + decl + ") is defined inline and is not a reference");
        }
        if (isResolved()) {
            throw new IllegalStateException("reference " + ref + " has already been resolved to " + schema);
        }
        AvroType referencedType = referencedSchema.type();
        if (!referencedType.isNamed()) {
            throw new IllegalArgumentException("cannot resolve " + ref + " to " + referencedSchema
                    + " because it's a " + referencedType);
        }
        AvroNamedSchema namedSchema = (AvroNamedSchema) referencedSchema;
        boolean isResolvedFromInheritedNamespace =
            getInheritedName() != null && getInheritedName().equals(namedSchema.getFullName());
        boolean isResolvedFromSimpleNamespace = ref.equals(namedSchema.getFullName());
        if (!isResolvedFromInheritedNamespace && !isResolvedFromSimpleNamespace) {
            //TODO - consider matching by aliases?
            throw new IllegalArgumentException("cannot resolve " + ref + " to " + namedSchema.getFullName());
        }
        schema = referencedSchema;
    }

    @Override
    public String toString() {
        if (ref != null) {
            String desc = "*" + ref;
            if (schema == null) {
                return desc + " (UNRESOLVED)";
            }
            return desc;
        }
        //noinspection ConstantConditions
        return decl.toString(); //!=null
    }
}
