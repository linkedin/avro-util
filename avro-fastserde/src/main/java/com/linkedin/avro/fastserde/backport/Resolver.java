/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.avro.fastserde.backport;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 * Backport of org.apache.avro.Resolver which was added in Avro 1.9
 *
 * Encapsulate schema-resolution logic in an easy-to-consume representation. See
 * resolve and also the separate document entitled
 * refactoring-resolution for more information. It might also be
 * helpful to study {@link org.apache.avro.io.parsing.ResolvingGrammarGenerator}
 * as an example of how to use this class.
 */
public class Resolver {

  /**
   * An abstract class for an action to be taken to resolve a writer's schema
   * (found in public instance variable writer) against a reader's schema
   * (in reader). Ordinarily, neither field can be null, except
   * that the reader field can be null in a Skip, which
   * is used to skip a field in a writer's record that doesn't exist in the
   * reader's (and thus there is no reader schema to resolve to).
   */
  public static abstract class Action {
    /** Helps us traverse faster. */
    public enum Type {
      DO_NOTHING, ERROR, PROMOTE, CONTAINER, ENUM, SKIP, RECORD, WRITER_UNION, READER_UNION
    }

    public final Schema writer, reader;
    public final Type type;

    protected Action(Schema w, Schema r, GenericData data, Type t) {
      this.writer = w;
      this.reader = r;
      this.type = t;
    }
  }

  /**
   * In this case there is an error. We put error Actions into trees because Avro
   * reports these errors in a lazy fashion: if a particular input doesn't
   * "tickle" the error (typically because it's in a branch of a union that isn't
   * found in the data being read), then it's safe to ignore it.
   */
  public static class ErrorAction extends Action {
    public enum ErrorType {
      /**
       * Use when Schema types don't match and can't be converted. For example,
       * resolving "int" and "enum".
       */
      INCOMPATIBLE_SCHEMA_TYPES,

      /**
       * Use when Schema types match but, in the case of record, enum, or fixed, the
       * names don't match.
       */
      NAMES_DONT_MATCH,

      /**
       * Use when two fixed types match and their names match by their sizes don't.
       */
      SIZES_DONT_MATCH,

      /**
       * Use when matching two records and the reader has a field with no default
       * value and that field is missing in the writer..
       */
      MISSING_REQUIRED_FIELD,

      /**
       * Use when matching a reader's union against a non-union and can't find a
       * branch that matches.
       */
      NO_MATCHING_BRANCH
    }

    public final ErrorType error;

    public ErrorAction(Schema w, Schema r, GenericData d, ErrorType e) {
      super(w, r, d, Action.Type.ERROR);
      this.error = e;
    }

    @Override
    public String toString() {
      switch (this.error) {
        case INCOMPATIBLE_SCHEMA_TYPES:
        case NAMES_DONT_MATCH:
        case SIZES_DONT_MATCH:
        case NO_MATCHING_BRANCH:
          return "Found " + AvroCompatibilityHelper.getSchemaFullName(writer) + ", expecting "
              + AvroCompatibilityHelper.getSchemaFullName(reader);

        case MISSING_REQUIRED_FIELD: {
          final List<Schema.Field> rfields = reader.getFields();
          String fname = "<oops>";
          for (Schema.Field rf : rfields) {
            if (writer.getField(rf.name()) == null && !AvroCompatibilityHelper.fieldHasDefault(rf)) {
              fname = rf.name();
            }
          }
          return ("Found " + AvroCompatibilityHelper.getSchemaFullName(writer) + ", expecting "
              + AvroCompatibilityHelper.getSchemaFullName(reader) + ", missing required field " + fname);
        }
        default:
          throw new IllegalArgumentException("Unknown error.");
      }
    }
  }

  /**
   * Contains information needed to resolve enumerations. When resolving enums,
   * adjustments need to be made in two scenarios: the index for an enum symbol
   * might be different in the reader or writer, or the reader might not have a
   * symbol that was written out for the writer (which is an error, but one we can
   * only detect when decoding data).
   *
   * These adjustments are reflected in the instance variable
   * adjustments. For the symbol with index i in the writer's
   * enum definition, adjustments[i] -- and integer -- contains the
   * adjustment for that symbol. If the integer is positive, then reader also has
   * the symbol and the integer is its index in the reader's schema. If
   * adjustment[i] is negative, then the reader does not have
   * the corresponding symbol (which is the error case).
   *
   * Sometimes there's no adjustments needed: all symbols in the reader have the
   * same index in the reader's and writer's schema. This is a common case, and it
   * allows for some optimization. To signal that this is the case,
   * noAdjustmentsNeeded is set to true.
   */
  public static class EnumAdjust extends Action {
    public final int[] adjustments;
    public final Object[] values;
    public final boolean noAdjustmentsNeeded;

    private EnumAdjust(Schema w, Schema r, GenericData d, int[] adj, Object[] values) {
      super(w, r, d, Action.Type.ENUM);
      this.adjustments = adj;
      boolean noAdj;
      int rsymCount = r.getEnumSymbols().size();
      int count = Math.min(rsymCount, adj.length);
      noAdj = (adj.length <= rsymCount);
      for (int i = 0; noAdj && i < count; i++) {
        noAdj &= (i == adj[i]);
      }
      this.noAdjustmentsNeeded = noAdj;
      this.values = values;
    }

    /**
     * If writer and reader don't have same name, a
     * {@link ErrorAction.ErrorType#NAMES_DONT_MATCH} is returned, otherwise an
     * appropriate {@link EnumAdjust} is.
     */
    public static Action resolve(Schema w, Schema r, GenericData d) {
      if (AvroCompatibilityHelper.getSchemaFullName(w) != null &&
          !AvroCompatibilityHelper.getSchemaFullName(w).equals(AvroCompatibilityHelper.getSchemaFullName(r)))
        return new ErrorAction(w, r, d, ErrorAction.ErrorType.NAMES_DONT_MATCH);

      final List<String> wsymbols = w.getEnumSymbols();
      final List<String> rsymbols = r.getEnumSymbols();
      final String enumDefault = AvroCompatibilityHelper.getEnumDefault(r);
      final int defaultIndex = (enumDefault == null ? -1 : rsymbols.indexOf(enumDefault));
      int[] adjustments = new int[wsymbols.size()];
      Object[] values = new Object[wsymbols.size()];
      Object defaultValue = (defaultIndex == -1) ? null : AvroCompatibilityHelper.newEnumSymbol(r, enumDefault);
      for (int i = 0; i < adjustments.length; i++) {
        int j = rsymbols.indexOf(wsymbols.get(i));
        if (j < 0) {
          j = defaultIndex;
        }
        adjustments[i] = j;
        values[i] = (j == defaultIndex) ? defaultValue : AvroCompatibilityHelper.newEnumSymbol(r, rsymbols.get(j));
      }
      return new EnumAdjust(w, r, d, adjustments, values);
    }
  }
}
