/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public interface LocatedCode {
    CodeLocation getCodeLocation();

    default TextLocation getStartLocation() {
        CodeLocation codeLocation = getCodeLocation();
        if (codeLocation == null) {
            return null;
        }
        return codeLocation.getStart();
    }

    default TextLocation getEndLocation() {
        CodeLocation codeLocation = getCodeLocation();
        if (codeLocation == null) {
            return null;
        }
        return codeLocation.getEnd();
    }
}
