/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

public enum DuplicateSchemaBehaviour {
  /**
   * warn about dups, but take no action.
   * <br>PLEASE AVOID IF AT ALL POSSIBLE.<br>
   * WARNING: this means that last-schema-visited determines the code generated.
   * given that the order of traversing schemas is undefined this could lead to
   * effectively-random java classes being generated.
   */
  WARN,
  /**
   * fail the process if the dup schema is defined differently (in any detail)
   * in 2+ locations. this is the default behaviour.
   */
  FAIL_IF_DIFFERENT,
  /**
   * fail the process on duplicate schemas, even if dups are identical.
   * from a craftsmanship perspective this is ideal, but a lot of projects
   * out there have dups
   */
  FAIL
}