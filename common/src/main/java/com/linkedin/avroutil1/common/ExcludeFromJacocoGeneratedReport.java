/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.common;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation that can be used to exclude code from jacoco coverage.
 *
 * Add @ExcludeFromJacocoGeneratedReport to the class or method that need to be excluded from jacoco coverage report.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PACKAGE, ElementType.TYPE, ElementType.ANNOTATION_TYPE, ElementType.METHOD,
    ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.LOCAL_VARIABLE, ElementType.PARAMETER})
public @interface ExcludeFromJacocoGeneratedReport {
}