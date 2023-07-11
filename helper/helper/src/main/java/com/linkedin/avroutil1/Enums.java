/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;

/**
 * Consists exclusively of static methods that operate on or return {@link Enum}s.
 */
public class Enums {
  private static final ClassValue<Enum<?>[]> ENUM_CONSTANTS = new ClassValue<Enum<?>[]>() {
    @Override
    protected Enum<?>[] computeValue(Class<?> type) {
      // JVM creates array everytime invoked.
      return (Enum<?>[]) type.getEnumConstants();
    }
  };

  private Enums() {
  }  // utility

  /**
   * Returns shared enum constant for specified ordinal.
   *
   * Lazily caches result of Enum#values to avoid unnecessary memory overhead.
   */
  public static <E extends Enum<?>> E getConstant(Class<E> type, int ordinal) {
    return (E) ENUM_CONSTANTS.get(type)[ordinal];
  }
}
