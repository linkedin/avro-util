/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

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
package com.linkedin.avroutil1.compatibility.avro111.parsing;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Symbol is the base of all symbols (terminals and non-terminals) of the
 * grammar.
 */
public abstract class Symbol {
  /*
   * The type of symbol.
   */
  public enum Kind {
    /** terminal symbols which have no productions */
    TERMINAL,
    /** Start symbol for some grammar */
    ROOT,
    /** non-terminal symbol which is a sequence of one or more other symbols */
    SEQUENCE,
    /** non-terminal to represent the contents of an array or map */
    REPEATER,
    /** non-terminal to represent the union */
    ALTERNATIVE,
    /** non-terminal action symbol which are automatically consumed */
    IMPLICIT_ACTION,
    /** non-terminal action symbol which is explicitly consumed */
    EXPLICIT_ACTION
  };

  /// The kind of this symbol.
  public final Kind kind;

  /**
   * The production for this symbol. If this symbol is a terminal this is
   * {@code null}. Otherwise this holds the the sequence of the symbols that
   * forms the production for this symbol. The sequence is in the reverse order of
   * production. This is useful for easy copying onto parsing stack.
   *
   * Please note that this is a final. So the production for a symbol should be
   * known before that symbol is constructed. This requirement cannot be met for
   * those symbols which are recursive (e.g. a record that holds union a branch of
   * which is the record itself). To resolve this problem, we initialize the
   * symbol with an array of nulls. Later we fill the symbols. Not clean, but
   * works. The other option is to not have this field a final. But keeping it
   * final and thus keeping symbol immutable gives some comfort. See various
   * generators how we generate records.
   */
  public final Symbol[] production;

  /**
   * Constructs a new symbol of the given kind {@code kind}.
   */
  protected Symbol(Kind kind) {
    this(kind, null);
  }

  protected Symbol(Kind kind, Symbol[] production) {
    this.production = production;
    this.kind = kind;
  }

  /**
   * A convenience method to construct a root symbol.
   */
  static Symbol root(Symbol... symbols) {
    return new Root(symbols);
  }

  /**
   * A convenience method to construct a sequence.
   *
   * @param production The constituent symbols of the sequence.
   */
  static Symbol seq(Symbol... production) {
    return new Sequence(production);
  }

  /**
   * A convenience method to construct a repeater.
   *
   * @param symsToRepeat The symbols to repeat in the repeater.
   */
  static Symbol repeat(Symbol endSymbol, Symbol... symsToRepeat) {
    return new Repeater(endSymbol, symsToRepeat);
  }

  /**
   * A convenience method to construct a union.
   */
  static Symbol alt(Symbol[] symbols, String[] oldLabels, String[] newLabels, boolean useFqcns) {
    return new Alternative(symbols, oldLabels, newLabels, useFqcns);
  }

  /**
   * A convenience method to construct an ErrorAction.
   *
   * @param e
   */
  static Symbol error(String e) {
    return new ErrorAction(e);
  }

  /**
   * A convenience method to construct a ResolvingAction.
   *
   * @param w The writer symbol
   * @param r The reader symbol
   */
  static Symbol resolve(Symbol w, Symbol r) {
    return new ResolvingAction(w, r);
  }

  private static class Fixup {
    public final Symbol[] symbols;
    public final int pos;

    public Fixup(Symbol[] symbols, int pos) {
      this.symbols = symbols;
      this.pos = pos;
    }
  }

  public Symbol flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
    return this;
  }

  public int flattenedSize() {
    return 1;
  }

  /**
   * Flattens the given sub-array of symbols into an sub-array of symbols. Every
   * {@code Sequence} in the input are replaced by its production recursively.
   * Non-{@code Sequence} symbols, they internally have other symbols those
   * internal symbols also get flattened. When flattening is done, the only place
   * there might be Sequence symbols is in the productions of a Repeater,
   * Alternative, or the symToParse and symToSkip in a UnionAdjustAction or
   * SkipAction.
   *
   * Why is this done? We want our parsers to be fast. If we left the grammars
   * unflattened, then the parser would be constantly copying the contents of
   * nested Sequence productions onto the parsing stack. Instead, because of
   * flattening, we have a long top-level production with no Sequences unless the
   * Sequence is absolutely needed, e.g., in the case of a Repeater or an
   * Alterantive.
   *
   * Well, this is not exactly true when recursion is involved. Where there is a
   * recursive record, that record will be "inlined" once, but any internal (ie,
   * recursive) references to that record will be a Sequence for the record. That
   * Sequence will not further inline itself -- it will refer to itself as a
   * Sequence. The same is true for any records nested in this outer recursive
   * record. Recursion is rare, and we want things to be fast in the typical case,
   * which is why we do the flattening optimization.
   *
   *
   * The algorithm does a few tricks to handle recursive symbol definitions. In
   * order to avoid infinite recursion with recursive symbols, we have a map of
   * Symbol->Symbol. Before fully constructing a flattened symbol for a
   * {@code Sequence} we insert an empty output symbol into the map and then
   * start filling the production for the {@code Sequence}. If the same
   * {@code Sequence} is encountered due to recursion, we simply return the
   * (empty) output {@code Sequence{@code  from the map. Then we actually fill out
   * the production for the {@code Sequence}. As part of the flattening process
   * we copy the production of {@code Sequence}s into larger arrays. If the
   * original {@code Sequence} has not not be fully constructed yet, we copy a
   * bunch of {@code null}s. Fix-up remembers all those {@code null} patches.
   * The fix-ups gets finally filled when we know the symbols to occupy those
   * patches.
   *
   * @param in    The array of input symbols to flatten
   * @param start The position where the input sub-array starts.
   * @param out   The output that receives the flattened list of symbols. The
   *              output array should have sufficient space to receive the
   *              expanded sub-array of symbols.
   * @param skip  The position where the output input sub-array starts.
   * @param map   A map of symbols which have already been expanded. Useful for
   *              handling recursive definitions and for caching.
   * @param map2  A map to to store the list of fix-ups.
   */
  static void flatten(Symbol[] in, int start, Symbol[] out, int skip, Map<Sequence, Sequence> map,
      Map<Sequence, List<Fixup>> map2) {
    for (int i = start, j = skip; i < in.length; i++) {
      Symbol s = in[i].flatten(map, map2);
      if (s instanceof Sequence) {
        Symbol[] p = s.production;
        List<Fixup> l = map2.get(s);
        if (l == null) {
          System.arraycopy(p, 0, out, j, p.length);
          // Copy any fixups that will be applied to p to add missing symbols
          for (List<Fixup> fixups : map2.values()) {
            copyFixups(fixups, out, j, p);
          }
        } else {
          l.add(new Fixup(out, j));
        }
        j += p.length;
      } else {
        out[j++] = s;
      }
    }
  }

  private static void copyFixups(List<Fixup> fixups, Symbol[] out, int outPos, Symbol[] toCopy) {
    for (int i = 0, n = fixups.size(); i < n; i += 1) {
      Fixup fixup = fixups.get(i);
      if (fixup.symbols == toCopy) {
        fixups.add(new Fixup(out, fixup.pos + outPos));
      }
    }
  }

  /**
   * Returns the amount of space required to flatten the given sub-array of
   * symbols.
   *
   * @param symbols The array of input symbols.
   * @param start   The index where the subarray starts.
   * @return The number of symbols that will be produced if one expands the given
   *         input.
   */
  protected static int flattenedSize(Symbol[] symbols, int start) {
    int result = 0;
    for (int i = start; i < symbols.length; i++) {
      if (symbols[i] instanceof Sequence) {
        Sequence s = (Sequence) symbols[i];
        result += s.flattenedSize();
      } else {
        result += 1;
      }
    }
    return result;
  }

  private static class Terminal extends Symbol {
    private final String printName;

    public Terminal(String printName) {
      super(Kind.TERMINAL);
      this.printName = printName;
    }

    @Override
    public String toString() {
      return printName;
    }
  }

  public static class ImplicitAction extends Symbol {
    /**
     * Set to {@code true} if and only if this implicit action is a trailing
     * action. That is, it is an action that follows real symbol. E.g
     * {@link Symbol#DEFAULT_END_ACTION}.
     */
    public final boolean isTrailing;

    private ImplicitAction() {
      this(false);
    }

    private ImplicitAction(boolean isTrailing) {
      super(Kind.IMPLICIT_ACTION);
      this.isTrailing = isTrailing;
    }
  }

  protected static class Root extends Symbol {
    private Root(Symbol... symbols) {
      super(Kind.ROOT, makeProduction(symbols));
      production[0] = this;
    }

    private static Symbol[] makeProduction(Symbol[] symbols) {
      Symbol[] result = new Symbol[flattenedSize(symbols, 0) + 1];
      flatten(symbols, 0, result, 1, new HashMap<>(), new HashMap<>());
      return result;
    }
  }

  protected static class Sequence extends Symbol implements Iterable<Symbol> {
    private Sequence(Symbol[] productions) {
      super(Kind.SEQUENCE, productions);
    }

    public Symbol get(int index) {
      return production[index];
    }

    public int size() {
      return production.length;
    }

    @Override
    public Iterator<Symbol> iterator() {
      return new Iterator<Symbol>() {
        private int pos = production.length;

        @Override
        public boolean hasNext() {
          return 0 < pos;
        }

        @Override
        public Symbol next() {
          if (0 < pos) {
            return production[--pos];
          } else {
            throw new NoSuchElementException();
          }
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public Sequence flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      Sequence result = map.get(this);
      if (result == null) {
        result = new Sequence(new Symbol[flattenedSize()]);
        map.put(this, result);
        List<Fixup> l = new ArrayList<>();
        map2.put(result, l);

        flatten(production, 0, result.production, 0, map, map2);
        for (Fixup f : l) {
          System.arraycopy(result.production, 0, f.symbols, f.pos, result.production.length);
        }
        map2.remove(result);
      }
      return result;
    }

    @Override
    public final int flattenedSize() {
      return flattenedSize(production, 0);
    }
  }

  public static class Repeater extends Symbol {
    public final Symbol end;

    private Repeater(Symbol end, Symbol... sequenceToRepeat) {
      super(Kind.REPEATER, makeProduction(sequenceToRepeat));
      this.end = end;
      production[0] = this;
    }

    private static Symbol[] makeProduction(Symbol[] p) {
      Symbol[] result = new Symbol[p.length + 1];
      System.arraycopy(p, 0, result, 1, p.length);
      return result;
    }

    @Override
    public Repeater flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      Repeater result = new Repeater(end, new Symbol[flattenedSize(production, 1)]);
      flatten(production, 1, result.production, 1, map, map2);
      return result;
    }

  }

  /**
   * Returns true if the Parser contains any Error symbol, indicating that it may
   * fail for some inputs.
   */
  public static boolean hasErrors(Symbol symbol) {
    return hasErrors(symbol, new HashSet<>());
  }

  private static boolean hasErrors(Symbol symbol, Set<Symbol> visited) {
    // avoid infinite recursion
    if (visited.contains(symbol)) {
      return false;
    }
    visited.add(symbol);

    switch (symbol.kind) {
    case ALTERNATIVE:
      return hasErrors(symbol, ((Alternative) symbol).symbols, visited);
    case EXPLICIT_ACTION:
      return false;
    case IMPLICIT_ACTION:
      if (symbol instanceof ErrorAction) {
        return true;
      }

      if (symbol instanceof UnionAdjustAction) {
        return hasErrors(((UnionAdjustAction) symbol).symToParse, visited);
      }

      return false;
    case REPEATER:
      Repeater r = (Repeater) symbol;
      return hasErrors(r.end, visited) || hasErrors(symbol, r.production, visited);
    case ROOT:
    case SEQUENCE:
      return hasErrors(symbol, symbol.production, visited);
    case TERMINAL:
      return false;
    default:
      throw new RuntimeException("unknown symbol kind: " + symbol.kind);
    }
  }

  private static boolean hasErrors(Symbol root, Symbol[] symbols, Set<Symbol> visited) {
    if (null != symbols) {
      for (Symbol s : symbols) {
        if (s == root) {
          continue;
        }
        if (hasErrors(s, visited)) {
          return true;
        }
      }
    }
    return false;
  }

  public static class Alternative extends Symbol {
    public final Symbol[] symbols;

    public final String[] oldLabels; //labels used in avro <= 1.4
    public final String[] newLabels; //labels used in more modern avro
    public final boolean useFqcns;

    private Alternative(Symbol[] symbols, String[] oldLabels, String[] newLabels, boolean useFqcns) {
      super(Kind.ALTERNATIVE);
      this.symbols = symbols;
      this.oldLabels = oldLabels;
      this.newLabels = newLabels;
      this.useFqcns = useFqcns;
    }

    public Symbol getSymbol(int index) {
      return symbols[index];
    }

    public String getLabel(int index) {
      return useFqcns ? newLabels[index] : oldLabels[index];
    }

    public int size() {
      return symbols.length;
    }

    public int findLabel(String label) {
      if (label != null) {
        if (!label.contains(".")) {
          for (int i = 0; i < oldLabels.length; i++) {
            if (label.equals(oldLabels[i])) {
              return i;
            }
          }
        } else {
          for (int i = 0; i < newLabels.length; i++) {
            if (label.equals(newLabels[i])) {
              return i;
            }
          }
        }
      }
      return -1;
    }

    @Override
    public Alternative flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      Symbol[] ss = new Symbol[symbols.length];
      for (int i = 0; i < ss.length; i++) {
        ss[i] = symbols[i].flatten(map, map2);
      }
      return new Alternative(ss, oldLabels, newLabels, useFqcns);
    }
  }

  public static class ErrorAction extends ImplicitAction {
    public final String msg;

    private ErrorAction(String msg) {
      this.msg = msg;
    }
  }

  public static IntCheckAction intCheckAction(int size) {
    return new IntCheckAction(size);
  }

  public static class IntCheckAction extends Symbol {
    public final int size;

    @Deprecated
    public IntCheckAction(int size) {
      super(Kind.EXPLICIT_ACTION);
      this.size = size;
    }
  }

  public static EnumAdjustAction enumAdjustAction(int rsymCount, Object[] adj) {
    return new EnumAdjustAction(rsymCount, adj);
  }

  public static class EnumAdjustAction extends IntCheckAction {
    public final boolean noAdjustments;
    public final Object[] adjustments;

    @Deprecated
    public EnumAdjustAction(int rsymCount, Object[] adjustments) {
      super(rsymCount);
      this.adjustments = adjustments;
      boolean noAdj = true;
      if (adjustments != null) {
        int count = Math.min(rsymCount, adjustments.length);
        noAdj = (adjustments.length <= rsymCount);
        for (int i = 0; noAdj && i < count; i++)
          noAdj &= ((adjustments[i] instanceof Integer) && i == (Integer) adjustments[i]);
      }
      this.noAdjustments = noAdj;
    }
  }

  public static WriterUnionAction writerUnionAction() {
    return new WriterUnionAction();
  }

  public static class WriterUnionAction extends ImplicitAction {
    private WriterUnionAction() {
    }
  }

  public static class ResolvingAction extends ImplicitAction {
    public final Symbol writer;
    public final Symbol reader;

    private ResolvingAction(Symbol writer, Symbol reader) {
      this.writer = writer;
      this.reader = reader;
    }

    @Override
    public ResolvingAction flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      return new ResolvingAction(writer.flatten(map, map2), reader.flatten(map, map2));
    }

  }

  public static SkipAction skipAction(Symbol symToSkip) {
    return new SkipAction(symToSkip);
  }

  public static class SkipAction extends ImplicitAction {
    public final Symbol symToSkip;

    @Deprecated
    public SkipAction(Symbol symToSkip) {
      super(true);
      this.symToSkip = symToSkip;
    }

    @Override
    public SkipAction flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      return new SkipAction(symToSkip.flatten(map, map2));
    }

  }

  public static FieldAdjustAction fieldAdjustAction(int rindex, String fname, Set<String> aliases) {
    return new FieldAdjustAction(rindex, fname, aliases);
  }

  public static class FieldAdjustAction extends ImplicitAction {
    public final int rindex;
    public final String fname;
    public final Set<String> aliases;

    @Deprecated
    public FieldAdjustAction(int rindex, String fname, Set<String> aliases) {
      this.rindex = rindex;
      this.fname = fname;
      this.aliases = aliases;
    }
  }

  public static class IntLongAdjustAction extends ImplicitAction {
    public static final IntLongAdjustAction INSTANCE = new IntLongAdjustAction();
  }

  public static FieldOrderAction fieldOrderAction(Schema.Field[] fields) {
    return new FieldOrderAction(fields);
  }

  public static final class FieldOrderAction extends ImplicitAction {
    public final boolean noReorder;
    public final Schema.Field[] fields;

    @Deprecated
    public FieldOrderAction(Schema.Field[] fields) {
      this.fields = fields;
      boolean noReorder = true;
      for (int i = 0; noReorder && i < fields.length; i++)
        noReorder &= (i == fields[i].pos());
      this.noReorder = noReorder;
    }
  }

  public static DefaultStartAction defaultStartAction(byte[] contents) {
    return new DefaultStartAction(contents);
  }

  public static class DefaultStartAction extends ImplicitAction {
    public final byte[] contents;

    @Deprecated
    public DefaultStartAction(byte[] contents) {
      this.contents = contents;
    }
  }

  public static UnionAdjustAction unionAdjustAction(int rindex, Symbol sym) {
    return new UnionAdjustAction(rindex, sym);
  }

  public static class UnionAdjustAction extends ImplicitAction {
    public final int rindex;
    public final Symbol symToParse;

    @Deprecated
    public UnionAdjustAction(int rindex, Symbol symToParse) {
      this.rindex = rindex;
      this.symToParse = symToParse;
    }

    @Override
    public UnionAdjustAction flatten(Map<Sequence, Sequence> map, Map<Sequence, List<Fixup>> map2) {
      return new UnionAdjustAction(rindex, symToParse.flatten(map, map2));
    }

  }

  /** For JSON. */
  public static EnumLabelsAction enumLabelsAction(List<String> symbols) {
    return new EnumLabelsAction(symbols);
  }

  public static class EnumLabelsAction extends IntCheckAction {
    public final List<String> symbols;

    @Deprecated
    public EnumLabelsAction(List<String> symbols) {
      super(symbols.size());
      this.symbols = symbols;
    }

    public String getLabel(int n) {
      return symbols.get(n);
    }

    public int findLabel(String l) {
      if (l != null) {
        for (int i = 0; i < symbols.size(); i++) {
          if (l.equals(symbols.get(i))) {
            return i;
          }
        }
      }
      return -1;
    }
  }

  /**
   * The terminal symbols for the grammar.
   */
  public static final Symbol NULL = new Terminal("null");
  public static final Symbol BOOLEAN = new Terminal("boolean");
  public static final Symbol INT = new Terminal("int");
  public static final Symbol LONG = new Terminal("long");
  public static final Symbol FLOAT = new Terminal("float");
  public static final Symbol DOUBLE = new Terminal("double");
  public static final Symbol STRING = new Terminal("string");
  public static final Symbol BYTES = new Terminal("bytes");
  public static final Symbol FIXED = new Terminal("fixed");
  public static final Symbol ENUM = new Terminal("enum");
  public static final Symbol UNION = new Terminal("union");

  public static final Symbol ARRAY_START = new Terminal("array-start");
  public static final Symbol ARRAY_END = new Terminal("array-end");
  public static final Symbol MAP_START = new Terminal("map-start");
  public static final Symbol MAP_END = new Terminal("map-end");
  public static final Symbol ITEM_END = new Terminal("item-end");

  public static final Symbol WRITER_UNION_ACTION = writerUnionAction();

  /* a pseudo terminal used by parsers */
  public static final Symbol FIELD_ACTION = new Terminal("field-action");

  public static final Symbol RECORD_START = new ImplicitAction(false);
  public static final Symbol RECORD_END = new ImplicitAction(true);
  public static final Symbol UNION_END = new ImplicitAction(true);
  public static final Symbol FIELD_END = new ImplicitAction(true);

  public static final Symbol DEFAULT_END_ACTION = new ImplicitAction(true);
  public static final Symbol MAP_KEY_MARKER = new Terminal("map-key-marker");
}
