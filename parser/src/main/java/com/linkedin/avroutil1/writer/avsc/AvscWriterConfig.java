/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

public class AvscWriterConfig {
  private final boolean pretty;
  /**
   * true to print horrible schemas recreating ancient avro behaviour from
   * before <a href="https://issues.apache.org/jira/browse/AVRO-702">AVRO-702</a>
   */
  private final boolean usePreAvro702Logic;
  /**
   * true to add aliases to all avro-702-impacted schemas.
   * aliases are either to the correct or the bad fullname,
   * depending on value of {@link #usePreAvro702Logic}.
   */
  private final boolean addAvro702Aliases;
  /**
   * avro specification says if namespace isn't defined its
   * inherited from parent, and hence does not emit namespace
   * on records who's namespace is the same as their parent's.
   * in addition to definitions, this also applies to references
   *
   * true changes this behaviour and always emits namespace,
   * even if its not strictly required by the specification.
   */
  private final boolean alwaysEmitNamespace;
  /**
   * true to emit "name" and "namespace" json properties.
   * false to always emit just "name" property for both
   * (which would then be either the simple name or the
   * fullname, as required)
   */
  private final boolean emitNamespacesSeparately;
  /**
   * true to define all nested schemas inline (aka "exploded schema").
   *
   * this is the only legal schema definition according to the avro
   * specification (which has no notion of imports/references).
   * this is also the only legal value of SCHEMA$ fields in generated
   * classes.
   *
   * false emits all nested schemas as a "reference" (a simple
   * fullname string). this is non-standard, but allows for common
   * schema reuse in large codebases.
   */
  private final boolean inlineAllNestedSchemas;

  public AvscWriterConfig(
      boolean pretty,
      boolean usePreAvro702Logic,
      boolean addAvro702Aliases,
      boolean alwaysEmitNamespace,
      boolean emitNamespacesSeparately,
      boolean inlineAllNestedSchemas
  ) {
    this.pretty = pretty;
    this.usePreAvro702Logic = usePreAvro702Logic;
    this.addAvro702Aliases = addAvro702Aliases;
    this.alwaysEmitNamespace = alwaysEmitNamespace;
    this.emitNamespacesSeparately = emitNamespacesSeparately;
    this.inlineAllNestedSchemas = inlineAllNestedSchemas;
    if (usePreAvro702Logic && alwaysEmitNamespace) {
      throw new IllegalArgumentException("cant specify usePreAvro702Logic and alwaysEmitNamespace together");
    }
  }

  public static final AvscWriterConfig CORRECT_MITIGATED = new AvscWriterConfig(
      false, false, true, false, true, true
  );

  public boolean isPretty() {
    return pretty;
  }

  public boolean isUsePreAvro702Logic() {
    return usePreAvro702Logic;
  }

  public boolean isAddAvro702Aliases() {
    return addAvro702Aliases;
  }

  public boolean isAlwaysEmitNamespace() {
    return alwaysEmitNamespace;
  }

  public boolean isEmitNamespacesSeparately() {
    return emitNamespacesSeparately;
  }

  public boolean isInlineAllNestedSchemas() {
    return inlineAllNestedSchemas;
  }
}
