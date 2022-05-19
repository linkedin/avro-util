/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * this class is used to keep track of all classes "inspected" as part of version detection heuristics.
 * used to provide more details in errors.
 */
public class VersionDetectionUtil {
  private final static Set<Class<?>> USED_FOR_CORE_AVRO_DETECTION = new HashSet<>(3);
  private final static Set<Class<?>> USED_FOR_AVRO_COMPILER_DETECTION = new HashSet<>(3);

  public static void markUsedForCoreAvro(Class<?> used) {
    USED_FOR_CORE_AVRO_DETECTION.add(used);
  }

  public static void markUsedForAvroCompiler(Class<?> used) {
    USED_FOR_AVRO_COMPILER_DETECTION.add(used);
  }

  public static List<String> uniqueSourcesForCoreAvro() {
    Set<String> protectionDomains = new HashSet<>(1);
    for (Class<?> clazz : USED_FOR_CORE_AVRO_DETECTION) {
      protectionDomains.add(clazz.getProtectionDomain().toString());
    }
    List<String> sorted = new ArrayList<>(protectionDomains);
    Collections.sort(sorted);
    return sorted;
  }

  public static List<String> uniqueSourcesForAvroCompiler() {
    Set<String> protectionDomains = new HashSet<>(1);
    for (Class<?> clazz : USED_FOR_AVRO_COMPILER_DETECTION) {
      protectionDomains.add(clazz.getProtectionDomain().toString());
    }
    List<String> sorted = new ArrayList<>(protectionDomains);
    Collections.sort(sorted);
    return sorted;
  }

}
