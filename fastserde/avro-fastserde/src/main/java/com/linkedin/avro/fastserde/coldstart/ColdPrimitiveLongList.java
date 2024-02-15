package com.linkedin.avro.fastserde.coldstart;

import com.linkedin.avro.api.PrimitiveLongList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


/**
 * A {@link PrimitiveLongList} implementation which is equivalent in all respect to the vanilla Avro
 * implementation, both in terms of functionality and (lack of) performance. It provides the primitive
 * API that the interface requires, but actually just returns an unboxed Float Object, thus providing
 * no GC benefit. This should be possible to improve upon in the future, however.
 * <br>
 * The main motivation for this class is merely to provide a guarantee that the extended API is always
 * available, even when Fast-Avro isn't warmed up yet.
 */
public class ColdPrimitiveLongList extends GenericData.Array<Long> implements PrimitiveLongList {
  private static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.LONG));
  public ColdPrimitiveLongList(int capacity) {
    super(capacity, SCHEMA);
  }

  @Override
  public long getPrimitive(int index) {
    return get(index);
  }

  @Override
  public boolean addPrimitive(long o) {
    return add(o);
  }

  @Override
  public long setPrimitive(int index, long o) {
    return set(index, o);
  }
}
