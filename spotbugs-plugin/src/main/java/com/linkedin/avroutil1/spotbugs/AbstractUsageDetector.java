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
import org.apache.bcel.classfile.LocalVariable;
import org.apache.bcel.classfile.LocalVariableTable;
import org.apache.bcel.classfile.Method;
import org.apache.bcel.generic.Type;

/**
 * parent class for detectors that alert on _ANY_ usage of a given class/interface
 */
public abstract class AbstractUsageDetector extends OpcodeStackDetector {
    protected final BugReporter bugReporter;
    protected final String targetFqcn;
    protected final String classConstant;
    protected final String bugType;

    protected AbstractUsageDetector(BugReporter bugReporter, String targetFqcn, String bugType) {
        if (bugReporter == null || targetFqcn == null || bugType == null) {
            throw new IllegalArgumentException("all arguments required");
        }
        this.bugReporter = bugReporter;
        this.targetFqcn = targetFqcn;
        this.classConstant = this.targetFqcn.replace('.', '/');
        this.bugType = bugType;
    }

    @Override
    public void visitClassContext(ClassContext classContext) {
        //1st call super so all the machinery works (including sawOpcode() below)
        super.visitClassContext(classContext);
        examineVariables(classContext);
    }

    @Override
    public void sawOpcode(int seen) {
        switch (seen) {
            case Const.INVOKESPECIAL:
                //constructor?
                break;
            case Const.NEW:
                //variable declaration
                break;
            case Const.CHECKCAST:
                //casting
                break;
            case Const.INSTANCEOF:
                //instanceof
                break;
            case Const.INVOKEVIRTUAL:
                //method invocation
                break;
            default:
                return;
        }
        if (getClassConstantOperand().equals(classConstant)) {
            BugInstance bug = new BugInstance(this, bugType, NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }

    protected void examineVariables(ClassContext classContext) {
        for (Method method : classContext.getMethodsInCallOrder()) {
            Type returnType = method.getReturnType();
            if (checkSignature(returnType.getSignature())) {
                BugInstance bug = new BugInstance(this, bugType, NORMAL_PRIORITY)
                        .addClassAndMethod(classContext.getJavaClass(), method);
                bugReporter.reportBug(bug);
            }
            if (method.isAbstract() || method.isNative()) {
                //method arguments show up in the local variables table below, but not for abstract/native methods
                Type[] argumentTypes = method.getArgumentTypes();
                for (Type argumentType : argumentTypes) {
                    if (checkSignature(argumentType.getSignature())) {
                        //TODO - figure out how to get argument name or index
                        BugInstance bug = new BugInstance(this, bugType, NORMAL_PRIORITY)
                                .addClassAndMethod(classContext.getJavaClass(), method);
                        bugReporter.reportBug(bug);
                    }
                }
                continue; //no variables to look at
            }
            LocalVariableTable localVariableTable = method.getLocalVariableTable(); //includes method args
            if (localVariableTable == null) {
                return;
            }
            for (LocalVariable variable : localVariableTable.getLocalVariableTable()) {
                if (checkSignature(variable.getSignature())) {
                    //TODO - figure out how to add source line number?
                    BugInstance bug = new BugInstance(this, bugType, NORMAL_PRIORITY)
                            .addClassAndMethod(classContext.getJavaClass(), method);
                    bugReporter.reportBug(bug);
                }
            }
        }
    }

    protected boolean checkSignature(String signature) {
        return signature.contains(classConstant);
    }
}
