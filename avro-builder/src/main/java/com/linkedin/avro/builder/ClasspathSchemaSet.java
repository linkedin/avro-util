/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.builder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * an implementation of SchemaSet that locates schemas by looking up avro-generated
 * specific classes on the classpath and returning their SCHEMA$ fields.
 */
public class ClasspathSchemaSet implements SchemaSet {
  private final Set<Schema> returned = new HashSet<>();

  @Override
  public int size() {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * @param name FQCN of a schema (so "com.acme.Foo")
   * @return the schema, if an vro-generated class of the given name was found on the classpath
   */
  @Override
  public synchronized Schema getByName(String name) {
    try {
      Class<?> clazz = Class.forName(name);
      Schema schema = findSchemaOnInstance(clazz);
      if (schema != null) {
        returned.add(schema);
        return schema;
      }
      schema = findSchemaOnClass(clazz);
      if (schema != null) {
        returned.add(schema);
        return schema;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private Schema findSchemaOnInstance(Class<?> clazz) {
    try {
      //on old avro (1.4) not all classes have a getSchema() method (for example, fixed classes)
      //so we cannot test for "instance of GenericContainer) or anything, we can just look
      //for the getSchema method (which we add to everything)
      Method getSchemaMethod = clazz.getMethod("getSchema");
      if (getSchemaMethod == null) {
        throw new IllegalStateException("should never happen");
      }
      Object instance = clazz.newInstance();
      Schema schema = (Schema) getSchemaMethod.invoke(instance);
      if (schema == null) {
        throw new IllegalStateException("got null schema from " + getSchemaMethod + " on " + instance);
      }
      return schema;
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      return null;
    }
  }

  private Schema findSchemaOnClass(Class<?> clazz) {
    try {
      //for some classes (like enums) avro generates a static getClassSchema() method
      Method getSchemaMethod = clazz.getMethod("getClassSchema");
      if (getSchemaMethod == null || !Modifier.isStatic(getSchemaMethod.getModifiers())) {
        throw new IllegalStateException(clazz.getCanonicalName() + ".getClassSchema() not static");
      }
      Schema schema = (Schema) getSchemaMethod.invoke(null);
      if (schema == null) {
        throw new IllegalStateException(clazz.getCanonicalName() + ".getClassSchema() returned null");
      }
      return schema;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return null;
    }
  }

  @Override
  public synchronized List<Schema> getAll() {
    //we return only those schemas previously explicitly looked-up, we dont scan the classpath
    return new ArrayList<>(returned);
  }

  @Override
  public void add(Schema schema) {
    throw new UnsupportedOperationException("not implemented");
  }
}