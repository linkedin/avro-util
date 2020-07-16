package com.linkedin.avro.fastserde.coldstart;

import com.linkedin.avro.api.PrimitiveIntList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


/**
 * A {@link PrimitiveIntList} implementation which is equivalent in all respect to the vanilla Avro
 * implementation, both in terms of functionality and (lack of) performance. It provides the primitive
 * API that the interface requires, but actually just returns an unboxed Float Object, thus providing
 * no GC benefit. This should be possible to improve upon in the future, however.
 *
 * The main motivation for this class is merely to provide a guarantee that the extended API is always
 * available, even when Fast-Avro isn't warmed up yet.
 */
public class ColdPrimitiveIntList extends GenericData.Array<Integer> implements PrimitiveIntList {
  private static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.FLOAT));
  public ColdPrimitiveIntList(int capacity) {
    super(capacity, SCHEMA);
  }

  @Override
  public int getPrimitive(int index) {
    return get(index);
  }

  @Override
  public boolean addPrimitive(int o) {
    return add(o);
  }

  @Override
  public int setPrimitive(int index, int o) {
    return set(index, o);
  }
}
