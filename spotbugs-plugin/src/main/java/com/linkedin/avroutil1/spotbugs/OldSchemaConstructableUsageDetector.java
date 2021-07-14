/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.ba.ClassContext;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import org.apache.bcel.Const;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.classfile.LocalVariable;
import org.apache.bcel.classfile.LocalVariableTable;
import org.apache.bcel.classfile.Method;
import org.apache.bcel.generic.Type;

/**
 * detects usage of org.apache.avro.specific.SpecificDatumReader.SchemaConstructable,
 * which has been moved in avro 1.6+ (to org.apache.avro.specific.SpecificData.SchemaConstructable)
 */
public class OldSchemaConstructableUsageDetector extends OpcodeStackDetector {
    public static final String OLD_SCHEMACONSTRUCTABLE_FQCN = "org.apache.avro.specific.SpecificDatumReader$SchemaConstructable";
    public static final String OLD_SCHEMACONSTRUCTABLE_CLASSCONSTANT = OLD_SCHEMACONSTRUCTABLE_FQCN.replace('.', '/');
    public static final String BUG_TYPE = "OLD_SCHEMACONSTRUCTABLE_USAGE";

    private final BugReporter bugReporter;

    public OldSchemaConstructableUsageDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void visitClassContext(ClassContext classContext) {
        //1st call super so all the machinery works (including sawOpcode() below)
        super.visitClassContext(classContext);
        lookForSchemaConstructableImplementations(classContext);
        lookForSchemaConstructableVariables(classContext);
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.CHECKCAST) {
            return;
        }
        if (getClassConstantOperand().equals(OLD_SCHEMACONSTRUCTABLE_CLASSCONSTANT)) {
            // new JsonEncoder call
            BugInstance bug = new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }

    /**
     * check to see if current class implements old SchemaConstructable
     * @param classContext
     */
    protected void lookForSchemaConstructableImplementations(ClassContext classContext) {
        JavaClass javaClass = classContext.getJavaClass();
        try {
            //only look at directly implemented interfaces so we dont error out
            //for entire class hierarchies over a single bad parent
            for (String fqcn : javaClass.getInterfaceNames()) {
                if (OLD_SCHEMACONSTRUCTABLE_FQCN.equals(fqcn)) {
                    BugInstance bug = new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                            .addClass(javaClass);
                    bugReporter.reportBug(bug);
                    return;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected void lookForSchemaConstructableVariables(ClassContext classContext) {
        for (Method method : classContext.getMethodsInCallOrder()) {
            Type returnType = method.getReturnType();
            if (checkSignature(returnType.getSignature())) {
                BugInstance bug = new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                        .addClassAndMethod(classContext.getJavaClass(), method);
                bugReporter.reportBug(bug);
            }
            if (method.isAbstract() || method.isNative()) {
                //method arguments show up in the local variables table below, but not for abstract/native methods
                Type[] argumentTypes = method.getArgumentTypes();
                for (Type argumentType : argumentTypes) {
                    if (checkSignature(argumentType.getSignature())) {
                        //TODO - figure out how to get argument name or index
                        BugInstance bug = new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                                .addClassAndMethod(classContext.getJavaClass(), method);
                        bugReporter.reportBug(bug);
                    }
                }
                continue; //no variables to look at
            }
            LocalVariableTable localVariableTable = method.getLocalVariableTable(); //includes method args
            for (LocalVariable variable : localVariableTable.getLocalVariableTable()) {
                if (checkSignature(variable.getSignature())) {
                    //TODO - figure out how to add sour line number?
                    BugInstance bug = new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                            .addClassAndMethod(classContext.getJavaClass(), method);
                    bugReporter.reportBug(bug);
                }
            }
        }
    }

    protected boolean checkSignature(String signature) {
        return signature.contains(OLD_SCHEMACONSTRUCTABLE_CLASSCONSTANT);
    }
}
