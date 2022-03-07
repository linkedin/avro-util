/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import java.util.List;


public class Avro702Data {
  private AvroName correctName; //actual true fullname of type
  private AvroName asWritten;   //effective (explicit or inherited) fullname as written
  private List<AvroName> extraAliases;  //any alias that needs to be added
  /**
   * the effective "context" namespace for any nested types (that do not explicitly state namespace)
   * as it would be when traversing the "correct" output AVSC
   */
  private String namespaceWhenParsing;
  /**
   * the effective "context" namespace for any nested types (that do not explicitly state namespace)
   * as it would be when traversing the output AVSC as old (avro-702) logic would print it
   */
  private String namespaceWhenParsing702;

  public Avro702Data(
      AvroName correctName,
      AvroName asWritten,
      List<AvroName> extraAliases,
      String namespaceWhenParsing,
      String namespaceWhenParsing702
  ) {
    this.correctName = correctName;
    this.asWritten = asWritten;
    this.extraAliases = extraAliases;
    this.namespaceWhenParsing = namespaceWhenParsing;
    this.namespaceWhenParsing702 = namespaceWhenParsing702;
  }

  public AvroName getCorrectName() {
    return correctName;
  }

  public AvroName getAsWritten() {
    return asWritten;
  }

  public List<AvroName> getExtraAliases() {
    return extraAliases;
  }

  public String getNamespaceWhenParsing() {
    return namespaceWhenParsing;
  }

  public String getNamespaceWhenParsing702() {
    return namespaceWhenParsing702;
  }
}
