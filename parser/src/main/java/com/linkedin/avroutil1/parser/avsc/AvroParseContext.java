/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.SchemaOrRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * maintains state for a "long" parse operation, involving
 * parsing multiple avro source files, which might reference each other
 */
public class AvroParseContext {
    // input

    private final List<AvscStandaloneResult> individualResults = new ArrayList<>(1);

    // output/state/calculated - calculated once

    /**
     * resolving references seales this context and prevents the addition of any more
     * individual file parse results
     */
    private boolean sealed = false;
    /**
     * all named (record, enum, fixed) schemas defined in any importable source file
     */
    private Map<String, AvscParseResult> importableNamedSchemas = new HashMap<>(1);
    /**
     * all named (record, enum, fixed) schemas defined in any source file
     */
    private Map<String, AvscParseResult> allNamedSchemas = new HashMap<>(1);
    /**
     * these are the top-level (outermost) schemas defined, one per file
     */
    private Set<AvroSchema> topLevelSchemas = new HashSet<>(1);
    private Map<String, List<AvscParseResult>> duplicates = new HashMap<>(1);
    /**
     * remaining unresolved references after all references across files that
     * are part of this context have been handled.
     */
    private List<SchemaOrRef> externalReferences = null;
    /**
     * remaining fields who's default values cannot be parsed after resolving all
     * references within this parse context (due to remaining unresolved references
     * in the field type)
     */
    private List<AvroSchemaField> fieldsWithUnparsedDefaults = null;

    @Deprecated
    public void add(AvscParseResult singleResult) {
        add(singleResult, true);
    }

    public void add(AvscParseResult singleResult, boolean isImportable) {
        if (singleResult == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        assertMutable();
        individualResults.add(new AvscStandaloneResult(singleResult, isImportable));

        //add to an index of FQCNs (also find dups)
        Throwable error = singleResult.getParseError();
        if (error != null) {
            //don't touch files with outright failures
            //TODO - add an issue here
            return;
        }
        Map<String, AvroNamedSchema> namedInFile = singleResult.getDefinedNamedSchemas();
        namedInFile.forEach((fqcn, schema) -> {
            AvscParseResult prior = allNamedSchemas.putIfAbsent(fqcn, singleResult);
            if (prior != null) {
                //TODO - find dups in aliases as well ?
                //this is a dup
                duplicates.compute(fqcn, (k, dups) -> {
                    if (dups == null) {
                        dups = new ArrayList<>(2);
                        dups.add(prior);
                    }
                    dups.add(singleResult);
                    return dups;
                });
                //TODO - add context-level issues for dups
            }
            if (isImportable) {
                //dont care about return value since we handled dups above
                importableNamedSchemas.putIfAbsent(fqcn, singleResult);
            }
        });
        topLevelSchemas.add(singleResult.getTopLevelSchema());
    }

    /**
     * expected to be called once after all individual file parsing results have been added.
     * this method resolves any cross-file references in schemas.
     * no file parsing results can be added to this context once this method is called.
     */
    public void resolveReferences() {
        assertMutable();
        sealed = true;

        //resolve any unresolved references in individual file results from other files
        externalReferences = new ArrayList<>();
        for (AvscStandaloneResult singleFile : individualResults) {
            Set<SchemaOrRef> externalRefs = singleFile.parseResult.getExternalReferences();
            for (SchemaOrRef ref : externalRefs) {
                String simpleName = ref.getRef();
                AvscParseResult simpleNameResolution = importableNamedSchemas.get(simpleName);
                AvscParseResult inheritedNameResolution = null;

                String inheritedName = ref.getInheritedName();
                if (inheritedName != null) {
                    inheritedNameResolution = importableNamedSchemas.get(inheritedName);
                }

                // The namespace may be inherited from the parent schema's context or may already be defined in the
                // name. There may be multiple resolutions for a simple name (either from the null namespace or from
                // the inherited namespace).
                if (inheritedNameResolution != null) {
                    ref.setResolvedTo(inheritedNameResolution.getDefinedNamedSchemas().get(inheritedName));
                    if (simpleNameResolution != null) {
                        String msg =
                            "ERROR: Two different schemas found for reference " + simpleName + " with inherited name "
                                + inheritedName + ". Only one should exist.";
                        singleFile.parseResult.addIssue(new AvscIssue(ref.getCodeLocation(), IssueSeverity.WARNING, msg,
                            new IllegalStateException(msg)));
                    }
                } else if (simpleNameResolution != null) {
                    ref.setResolvedTo(simpleNameResolution.getDefinedNamedSchemas().get(simpleName));
                } else {
                    //fqcn is unresolved in this context
                    externalReferences.add(ref);
                }
            }
        }

        //now with (hopefully some) external references resolved go over unparsed default values and parse them
        fieldsWithUnparsedDefaults = new ArrayList<>();
        for (AvscStandaloneResult singleFile : individualResults) {
            AvscParseResult parseResult = singleFile.parseResult;
            Set<AvroSchemaField> incompleteFields = parseResult.getFieldsWithUnparsedDefaults();
            for (AvroSchemaField field : incompleteFields) {
                SchemaOrRef schemaOrRef = field.getSchemaOrRef();
                if (!schemaOrRef.isResolved()) {
                    fieldsWithUnparsedDefaults.add(field);
                    continue;
                }
                AvscParseUtil.lateParseFieldDefault(field, parseResult.getContext());
            }
        }
    }

    public Map<String, AvscParseResult> getAllNamedSchemas() {
        return allNamedSchemas;
    }

    public Set<AvroSchema> getTopLevelSchemas() {
        return topLevelSchemas;
    }

    public Map<String, List<AvscParseResult>> getDuplicates() {
        return duplicates;
    }

    public boolean hasExternalReferences() {
        assertSealed();
        return externalReferences != null && !externalReferences.isEmpty();
    }

    public List<SchemaOrRef> getExternalReferences() {
        assertSealed();
        return externalReferences;
    }

    public List<AvroSchemaField> getFieldsWithUnparsedDefaults() {
        assertSealed();
        return fieldsWithUnparsedDefaults;
    }

    private void assertSealed() {
        if (!sealed) {
            throw new IllegalStateException("this context has not yet been sealed");
        }
    }

    private void assertMutable() {
        if (sealed) {
            throw new IllegalStateException("this context has already been sealed");
        }
    }

    private class AvscStandaloneResult {
        AvscParseResult parseResult;
        boolean isImportable;
        AvscStandaloneResult(AvscParseResult parseResult, boolean isImportable) {
            this.parseResult = parseResult;
            this.isImportable = isImportable;
        }
    }
}
