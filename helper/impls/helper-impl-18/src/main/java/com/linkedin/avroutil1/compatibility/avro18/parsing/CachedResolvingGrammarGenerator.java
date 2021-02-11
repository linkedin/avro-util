/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18.parsing;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * The class that generates a resolving grammar to resolve between two
 * schemas.
 * A version of ResolvingGrammarGenerator with a resolveRecords that overrides the method in the base class
 * to use a fixed version of the SkipAction called CachedSkipAction
 */
public class CachedResolvingGrammarGenerator extends ResolvingGrammarGenerator {
  /**
   * Resolves the writer schema  and the reader schema
   * and returns the start symbol for the grammar generated.
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @return          The start symbol for the resolving grammar
   * @throws IOException
   */
  protected Symbol resolveRecords(Schema writer, Schema reader,
      Map<LitS, Symbol> seen) throws IOException {
    ValidatingGrammarGenerator.LitS wsc = new LitS2(writer, reader);
    Symbol result = seen.get(wsc);
    if (result == null) {
      List<Schema.Field> wfields = writer.getFields();
      List<Schema.Field> rfields = reader.getFields();
      // First, compute reordering of reader fields, plus
      // number elements in the result's production
      Schema.Field[] reordered = new Schema.Field[rfields.size()];
      int ridx = 0;
      int count = 1 + wfields.size();
      for (Schema.Field f : wfields) {
        Schema.Field rdrField = reader.getField(f.name());
        if (rdrField != null) {
          reordered[ridx++] = rdrField;
        }
      }
      for (Schema.Field rf : rfields) {
        String fname = rf.name();
        if (writer.getField(fname) == null) {
          if (rf.defaultValue() == null) {
            result = Symbol.error("Found " + writer + ", expecting " + reader);
            seen.put(wsc, result);
            return result;
          } else {
            reordered[ridx++] = rf;
            count= 3;
          }
        }
      }
      Symbol[] production = new Symbol[count];
      production[--count] = new Symbol.FieldOrderAction(reordered);
      /**
       * We construct a symbol without filling the array. Please see
       * {@link Symbol#production} for the reason.
       */
      result = Symbol.seq(production);
      seen.put(wsc, result);
      /*
       * For now every field in read-record with no default value
       * must be in write-record.
       * Write record may have additional fields, which will be
       * skipped during read.
       */
      // Handle all the writer's fields
      for (Schema.Field wf : wfields) {
        String fname = wf.name();
        Schema.Field rf = reader.getField(fname);
        if (rf == null) {
          production[--count] =
              new CachedSymbol.CachedSkipAction(generate(wf.schema(), wf.schema(), seen, true));
        } else {
          production[--count] =
              generate(wf.schema(), rf.schema(), seen, true);
        }
      }
      // Add default values for fields missing from Writer
      for (Schema.Field rf : rfields) {
        String fname = rf.name();
        Schema.Field wf = writer.getField(fname);
        if (wf == null) {
          byte[] bb = getBinary(rf.schema(), rf.defaultValue());
          production[--count] = new Symbol.DefaultStartAction(bb);
          production[--count] = generate(rf.schema(), rf.schema(), seen, true);
          production[--count] = Symbol.DEFAULT_END_ACTION;
        }
      }
    }
    return result;
  }
}