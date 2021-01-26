/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19.parsing;

import java.lang.reflect.Field;
import java.util.Map;


/**
 * A version of Symbol that has a fixed version of Field: TRAILING_FIELD
 * and a fixed version of the SkipAction: CachedSkipAction to work with TRAILING_FIELD to skip Symbol
 */
public class CachedSymbol extends Symbol {
  protected CachedSymbol(Kind kind) {
    this(kind, null);
  }

  protected CachedSymbol(Kind kind, Symbol[] production) {
    super(kind, production);
  }

  static final Field TRAILING_FIELD;
  static {
    try {
      TRAILING_FIELD = ImplicitAction.class.getDeclaredField("isTrailing");
      TRAILING_FIELD.setAccessible(true);
    } catch (Exception ex) {
      throw new IllegalStateException("Expecting class ImplicitAction to have field isTrailing. " + ex);
    }
  }

  public static class CachedSkipAction extends SkipAction {
    public CachedSkipAction(Symbol symToSkip) {
      super(symToSkip);
      try {
        TRAILING_FIELD.setBoolean(this, true);
      } catch (IllegalAccessException e) {
        throw new Error(e);
      }
    }
    @Override
    public SkipAction flatten(Map map, Map map2) {
      return new CachedSkipAction(symToSkip.flatten(map, map2));
    }
  }
}
