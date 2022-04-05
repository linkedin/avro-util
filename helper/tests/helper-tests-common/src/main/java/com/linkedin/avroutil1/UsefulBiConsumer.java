/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;

@FunctionalInterface
public interface UsefulBiConsumer<T, U> {
  //real men throw checked exceptions
  void accept(T t, U u) throws Exception;
}
