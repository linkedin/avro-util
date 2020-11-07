/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.avroutil1.compatibility.avro14.parsing;

import java.lang.reflect.Field;
import java.util.Map;


public class CachedSymbol extends Symbol {
  protected CachedSymbol(Kind kind) {
    this(kind, null);
  }

  protected CachedSymbol(Kind kind, Symbol[] production) {
    super(kind,production);
  }

  static final Field TRAILING_FIELD;
  static {
    try {
      TRAILING_FIELD = ImplicitAction.class.getDeclaredField("isTrailing");
    } catch (Throwable ex) {
      throw new Error(ex);
    }
    TRAILING_FIELD.setAccessible(true);
  }

  public static class CachedSkipAction extends SkipAction {
    public CachedSkipAction(Symbol symToSkip) {
      super(symToSkip);
      try
      {
        TRAILING_FIELD.setBoolean(this, true);
      } catch (IllegalAccessException e)
      {
        throw new Error(e);
      }
    }
    @Override
    public SkipAction flatten(Map map,
        Map map2) {
      return new CachedSkipAction(symToSkip.flatten(map, map2));
    }
  }
}

