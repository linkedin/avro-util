/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;


public class AvscWriterContext {

  /**
   * updates every time we "enter" into and exit out of a named type, this
   * keeps track of the namespace vs which any "unqualified" (==not full)
   * name(s) would be resolved.
   * to implement avro-702 mitigation it also keeps track of how 702-afflicted
   * records will be parsed
   */
  private final ArrayDeque<NamingContext> contextStack = new ArrayDeque<>(1);
  /**
   * latches to the 1st non-null namespace encountered, never pop()ed or reset
   */
  private String avro702ContextNamespace = null;


  private final Map<String, AvroNamedSchema> known = new HashMap<>(); //by their fullname

  public AvscWriterContext() {
    // Context starts at the "root" namespace ie. empty.
    contextStack.push(new NamingContext("", ""));
  }

  /**
   * @return the correct namespace at this current inner-most context
   */
  public String getCorrectContextNamespace() {
    assert contextStack.peek() != null;
    return contextStack.peek().correctNamespace;
  }

  /**
   * @return the namespace that old pre-702 avro logic would evaluate the current context namespace to be
   */
  public String getAvro702ContextNamespace() {
    return avro702ContextNamespace;
  }

  /**
   * registers a (possibly known) named schema
   * @param schema a name schema encountered during avsc generation
   * @return true if schema has already been encountered (and hence defined) before, false if schema is new (seen 1st time)
   */
  public boolean schemaEncountered(AvroNamedSchema schema) {
    String fullName = schema.getFullName();
    AvroSchema alreadySeen = known.get(fullName);
    if (alreadySeen == null) {
      known.put(fullName, schema);
      return false;
    }
    //make sure we dont have a different redefinition
    if (!alreadySeen.equals(schema)) {
      throw new IllegalStateException("schema " + fullName + " at " + schema.getCodeLocation()
          + " already seen (and different) at " + alreadySeen.getCodeLocation());
    }
    return true;
  }

  public void pushNamingContext(String correctNamespace, String pre702ParsedNamespace) {
    contextStack.push(new NamingContext(correctNamespace, pre702ParsedNamespace));
  }

  public void popNamingContext() {
    contextStack.pop(); //will throw if empty
  }

  private static class NamingContext {
    /**
     * the correct namespace of this context, as defined by the avro specification
     */
    private final String correctNamespace;
    /**
     * what would be the context namespace here on parsing if the schema was
     * written under pre-702 (incorrect) logic
     */
    private final String pre702ParsedNamespace;

    public NamingContext(String correctNamespace, String pre702ParsedNamespace) {
      this.correctNamespace = correctNamespace;
      this.pre702ParsedNamespace = pre702ParsedNamespace;
    }
  }
}
