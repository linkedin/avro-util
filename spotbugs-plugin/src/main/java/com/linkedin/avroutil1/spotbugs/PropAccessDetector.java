/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.classfile.MethodDescriptor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.bcel.Const;

/**
 * detects access to the following methods on org.apache.avro.Schema,
 * org.apache.avro.Schema#Field and org.apache.avro.JsonProperties:
 * <ul>
 *     <li>getJsonProp() - returns jackson1 JsonNode in avro 1.7.3, became private in 1.9 (and moved to jackson2)</li>
 *     <li>getObjectProp() - in avro 1.8+</li>
 *     <li>addProp(JsonNode) - uses jackson1 in avro 1.7.3, became private in 1.9 (and moved to jackson2)</li>
 *     <li>addProp(Object) - in avro 1.8+</li>
 *     <li>props() - avro 1.6 to 1.8</li>
 *     <li>getJsonProps() - uses jackson1 in avro 1.7.3 to 1.8</li>
 *     <li>getObjectProps() - in avro 1.8+</li>
 *     <li>addAllProps() - in avro 1.9+</li>
 * </ul>
 * the getProp(Str) and addProp(Str) methods exist in all avro versions (hence are compatible),
 * but only operate on string props
 */
public class PropAccessDetector extends AvroUtilDetectorBase {
    private final BugReporter bugReporter;
    private final static Set<String> CLASSES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "org/apache/avro/Schema",
            "org/apache/avro/Schema$Field",
            "org/apache/avro/JsonProperties"
    )));
    private final static Set<String> METHOD_NAMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "getProps",
            "getJsonProps",
            "getObjectProps",
            "getJsonProp",
            "getObjectProp",
            "addAllProps"
    )));

    public PropAccessDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.INVOKEVIRTUAL) {
            return;
        }
        String classConstantOperand = getClassConstantOperand();
        if (!CLASSES.contains(classConstantOperand)) {
            return;
        }
        MethodDescriptor method = getMethodDescriptorOperand();
        String methodName = method.getName();
        boolean fileBug = false;
        if (METHOD_NAMES.contains(methodName)) {
            fileBug = true;
        } else if (methodName.equals("addProp")) {
            //make sure its NOT the string prop one
            String signature = method.getSignature();
            if (signature.contains("JsonNode") || signature.contains("Object")) {
                fileBug = true;
            }
        }

        if (fileBug) {
            // incompatible prop access
            BugInstance bug = new BugInstance(this, "PROP_ACCESS", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
