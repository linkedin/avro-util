/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * maintains state for a "long" parse operation, involving
 * parsing multiple avro source files, which might reference each other
 */
public class AvroParseContext {
    // input

    private final List<AvscStandaloneResult> individualResults = new ArrayList<>(1);

    // output/state/calculated - calculated once

    private boolean sealed = false;
    private Map<String, AvscParseResult> knownImportableSchemas = null;
    private Map<String, List<AvscParseResult>> duplicates = null;
    private List<SchemaOrRef> externalReferences = null;

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
    }

    public void resolveReferences() {
        sealed = true;

        //build up an index of FQCNs (also find dups)
        knownImportableSchemas = new HashMap<>(individualResults.size());
        duplicates = new HashMap<>(1);
        for (AvscStandaloneResult singleFile : individualResults) {
            Throwable error = singleFile.parseResult.getParseError();
            if (error != null) {
                //don't touch files with outright failures
                continue;
            }
            Map<String, AvroSchema> namedInFile = singleFile.parseResult.getDefinedNamedSchemas();
            namedInFile.forEach((fqcn, schema) -> {
                AvscParseResult firstDefinition = knownImportableSchemas.get(fqcn);
                if (singleFile.isImportable) {
                   knownImportableSchemas.putIfAbsent(fqcn, singleFile.parseResult);
                }
                if (firstDefinition != null) {
                    //TODO - find dups in aliases as well ?
                    //this is a dup
                    duplicates.compute(fqcn, (k, dups) -> {
                        if (dups == null) {
                            dups = new ArrayList<>(2);
                            dups.add(firstDefinition);
                        }
                        dups.add(singleFile.parseResult);
                        return dups;
                    });
                }

            });
        }
        //TODO - add context-level issues for dups

        //resolve any unresolved references in individual file results from other files
        externalReferences = new ArrayList<>();
        for (AvscStandaloneResult singleFile : individualResults) {
            List<SchemaOrRef> externalRefs = singleFile.parseResult.getExternalReferences();
            for (SchemaOrRef ref : externalRefs) {
                String simpleName = ref.getRef();
                AvscParseResult simpleNameResolution = knownImportableSchemas.get(simpleName);
                AvscParseResult inheritedNameResolution = null;

                String inheritedName = ref.getInheritedName();
                if (inheritedName != null) {
                    inheritedNameResolution = knownImportableSchemas.get(inheritedName);
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
    }

    public boolean hasExternalReferences() {
        assertSealed();
        return externalReferences != null && !externalReferences.isEmpty();
    }

    public List<SchemaOrRef> getExternalReferences() {
        assertSealed();
        return externalReferences;
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
