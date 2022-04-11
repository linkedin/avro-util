/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import edu.umd.cs.findbugs.classfile.ClassDescriptor;
import edu.umd.cs.findbugs.classfile.MethodDescriptor;
import org.apache.bcel.Const;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * detects instantiations of org.apache.avro.io.DatumReaders of various sorts using only a single schema:
 * <ul>
 *     <li>org.apache.avro.generic.GenericDatumReader</li>
 *     <li>org.apache.avro.specific.SpecificDatumReader</li>
 *     <li>com.linkedin.avro.fastserde.FastGenericDatumReader</li>
 *     <li>com.linkedin.avro.fastserde.FastSpecificDatumReader</li>
 * </ul>
 * although there are valid cases for doing this, in real life this tends to indicate naive code that does not
 * take any schema evolution into considerations (hence assumes reader and writer schemas to be the same)
 */
public class SingleSchemaDeserializationDetector extends OpcodeStackDetector {
    private final static Set<String> READER_FQCNS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "org.apache.avro.generic.GenericDatumReader",
            "org.apache.avro.specific.SpecificDatumReader",
            "com.linkedin.avro.fastserde.FastGenericDatumReader",
            "com.linkedin.avro.fastserde.FastSpecificDatumReader"
    )));
    private final BugReporter bugReporter;

    public SingleSchemaDeserializationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKESPECIAL) { //constructor call
            return;
        }
        ClassDescriptor classBeingConstructed = getClassDescriptorOperand();
        //in theory we could at this point check to see if classBeingConstructed implements DatumReader
        //but avro isnt always in the context of the analysis (for example in our test setup) and that would
        //make this detector more fragile. instead we just pack a set of known DatumReader FQCNs.
        String fqcn = classBeingConstructed.getDottedClassName();
        if (!READER_FQCNS.contains(fqcn)) {
            return;
        }
        MethodDescriptor constructorDesc = getMethodDescriptorOperand();
        int numSchemaArgs = occurrences("org/apache/avro/Schema", constructorDesc.getSignature());
        if (numSchemaArgs == 1
            && occurrences("GenericDatumReader", String.valueOf(getXClass().getSuperclassDescriptor())) == 0) {
            // single schema arg constructor
            BugInstance bug = new BugInstance(this, "SINGLE_SCHEMA_DESERIALIZATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }

    //TODO - move to common StringUtils
    int occurrences(@SuppressWarnings("SameParameterValue") String needle, String hayStack) {
        int occurrences = 0;
        int index = hayStack.indexOf(needle);
        while (index != -1) {
            occurrences++;
            index = hayStack.indexOf(needle, index + needle.length()); //no overlaps
        }
        return occurrences;
    }
}
