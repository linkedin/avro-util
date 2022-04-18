/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

public class ClassWithStaticInitIssues {
    static {
        //this doesnt actually prevent class LOADING, but will throw
        //ExceptionInInitializerError (on 1st) or NoClassDefFoundError (on 2nd+)
        //when any attempts are made to instantiate this class
        if (true) {
            throw new IllegalStateException("nope");
        }
    }

    public ClassWithStaticInitIssues() {
    }
}
