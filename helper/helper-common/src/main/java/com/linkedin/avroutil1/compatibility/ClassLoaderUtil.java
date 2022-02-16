/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

public class ClassLoaderUtil {
    private ClassLoaderUtil() {
        //util class
    }

    public static Class<?> forName(String className) throws ClassNotFoundException {
        return forName(null, className);
    }

    public static Class<?> forName(ClassLoader classLoader, String className) throws ClassNotFoundException {
        if (className == null || className.isEmpty()) {
            throw new IllegalArgumentException("className required");
        }
        Class<?> c = null;
        ClassNotFoundException issue = null;
        try {
            c = loadClass(classLoader, className);
        } catch (ClassNotFoundException e) {
            issue = e;
        }
        if (c != null) {
            return c;
        }
        try {
            c = loadClass(Thread.currentThread().getContextClassLoader(), className);
        } catch (ClassNotFoundException e) {
            if (issue == null) {
                issue = e;
            } else {
                issue.addSuppressed(e);
            }
        }

        if (c != null) {
            return c;
        }

        throw new ClassNotFoundException("Failed to load class" + className, issue);
    }

    private static Class<?> loadClass(ClassLoader classLoader, String className) throws ClassNotFoundException {
        if (classLoader == null) {
            return null;
        }
        return Class.forName(className, true, classLoader);
    }
}
