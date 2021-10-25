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

    private List<AvscParseResult> individualResults = new ArrayList<>(1);

    // output/state/calculated - calculated once

    private boolean sealed = false;
    private Map<String, AvscParseResult> knownNamedSchemas = null;
    private Map<String, List<AvscParseResult>> duplicates = null;
    private Map<String, List<SchemaOrRef>> externalReferences = null;

    public void add(AvscParseResult singleResult) {
        if (singleResult == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        assertMutable();
        individualResults.add(singleResult);
    }

    public void resolveReferences() {
        sealed = true;

        //build up an index of FQCNs (also find dups)
        knownNamedSchemas = new HashMap<>(individualResults.size());
        duplicates = new HashMap<>(1);
        for (AvscParseResult singleFile : individualResults) {
            Throwable error = singleFile.getParseError();
            if (error != null) {
                //dont touch files with outright failures
                continue;
            }
            Map<String, AvroSchema> namedInFile = singleFile.getDefinedNamedSchemas();
            namedInFile.forEach((fqcn, schema) -> {
                AvscParseResult firstDefinition = knownNamedSchemas.putIfAbsent(fqcn, singleFile);
                if (firstDefinition != null) {
                    //TODO - find dups in aliases as well ?
                    //this is a dup
                    duplicates.compute(fqcn, (k, dups) -> {
                        if (dups == null) {
                            dups = new ArrayList<>(2);
                            dups.add(firstDefinition);
                        }
                        dups.add(singleFile);
                        return dups;
                    });
                }

            });
        }
        //TODO - add context-level issues for dups

        //resolve any unresolved references in individual file results from other files
        externalReferences = new HashMap<>(1);
        for (AvscParseResult singleFile : individualResults) {
            Map<String, List<SchemaOrRef>> externalRefs = singleFile.getExternalReferences();
            externalRefs.forEach((fqcn, refs) -> {
                AvscParseResult otherFile = knownNamedSchemas.get(fqcn);
                if (otherFile == null) {
                    //fqcn is unresolved in this context
                    externalRefs.computeIfAbsent(fqcn, s -> new ArrayList<>(1)).addAll(refs);
                    return;
                }
                AvroSchema definition = otherFile.getDefinedNamedSchemas().get(fqcn);
                refs.forEach(ref -> ref.setResolvedTo(definition));
            });
        }
    }

    private void assertMutable() {
        if (sealed) {
            throw new IllegalStateException("this context has already been sealed");
        }
    }
}
