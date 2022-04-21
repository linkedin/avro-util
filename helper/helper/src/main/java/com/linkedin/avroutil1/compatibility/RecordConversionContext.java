/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Collection;
import java.util.Collections;
import org.apache.avro.Schema;


/**
 * state for a single conversion operation
 */
public class RecordConversionContext {
  private final RecordConversionConfig config;
  private boolean useSpecifics; //true means conversion is into specific classes
  private ClassLoader classLoader;

  public RecordConversionContext(RecordConversionConfig config) {
    this.config = config;
  }

  public RecordConversionConfig getConfig() {
    return config;
  }

  public boolean isUseSpecifics() {
    return useSpecifics;
  }

  public void setUseSpecifics(boolean useSpecifics) {
    this.useSpecifics = useSpecifics;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  /**
   * looks up a specific class given a schema on the context classloader
   * @return a java class, or null if none found matching criteria
   * TODO - describe exceptions
   */
  public Class<?> lookup(Schema schema) {

    Schema.Type type = schema.getType();
    if (!HelperConsts.NAMED_TYPES.contains(type)) {
      throw new IllegalArgumentException("dont know how to lookup " + type);
    }
    String fqcn = schema.getFullName();
    Collection<String> altFqcns = config.isUseAliasesOnNamedTypes() ? schema.getAliases() : Collections.emptyList();

    //todo - validate expectedType
    return load(fqcn, altFqcns, config.isValidateAliasUniqueness());
  }

  //protected FOR TESTING
  protected Class<?> load(
      String fqcn,
      Collection<String> altFqcns,
      boolean validateAltUniqueness
  ) {
    ClassNotFoundException directNotFoundException;
    try {
      //todo - validate expectedType
      return classLoader.loadClass(fqcn);
    } catch (ClassNotFoundException nfe) {
      directNotFoundException = nfe;
    }
    if (altFqcns == null || altFqcns.isEmpty()) {
      return null;
    }
    Class<?> altHit = null;
    for (String altFqcn : altFqcns) {
      try {
        Class<?> hit = classLoader.loadClass(altFqcn);
        if (validateAltUniqueness) {
          if (altHit != null) {
            IllegalStateException e = new IllegalStateException("2+ valid aliases found for " + fqcn + ": " + altHit.getName() + " and " + altFqcn);
            e.addSuppressed(directNotFoundException); //must exist if we got here
            throw e;
          }
          altHit = hit;
        } else {
          return hit;
        }
      } catch (ClassNotFoundException ignored) {
        //ignored
      }
    }

    return altHit; //could be null
  }
}
