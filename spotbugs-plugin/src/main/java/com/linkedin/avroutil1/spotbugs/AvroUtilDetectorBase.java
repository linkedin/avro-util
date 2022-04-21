/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public abstract class AvroUtilDetectorBase extends OpcodeStackDetector {
  private final static Set<String> GENERATED_PARENT_CLASSES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "org.apache.avro.specific.SpecificRecordBase",
      "org.apache.avro.specific.SpecificFixed"
  )));



  @Override
  public boolean beforeOpcode(int seen) {
    return super.beforeOpcode(seen) && isGenerated();
  }

  protected int occurrences(@SuppressWarnings("SameParameterValue") String needle, String hayStack) {
    int occurrences = 0;
    int index = hayStack.indexOf(needle);
    while (index != -1) {
      occurrences++;
      index = hayStack.indexOf(needle, index + needle.length()); //no overlaps
    }
    return occurrences;
  }

  private boolean isGenerated() {
    return (getXClass().getSuperclassDescriptor() == null) || !GENERATED_PARENT_CLASSES.contains(
        getXClass().getSuperclassDescriptor().getDottedClassName());
  }
}
