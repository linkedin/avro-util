package com.linkedin.avro.fastserde.coldstart;

import com.linkedin.avro.api.PrimitiveDoubleList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


/**
 * A {@link PrimitiveDoubleList} implementation which is equivalent in all respect to the vanilla Avro
 * implementation, both in terms of functionality and (lack of) performance. It provides the primitive
 * API that the interface requires, but actually just returns an unboxed Float Object, thus providing
 * no GC benefit. This should be possible to improve upon in the future, however.
 *
 * The main motivation for this class is merely to provide a guarantee that the extended API is always
 * available, even when Fast-Avro isn't warmed up yet.
 */
public class ColdPrimitiveDoubleList extends GenericData.Array<Double> implements PrimitiveDoubleList {
  private static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.FLOAT));
  public ColdPrimitiveDoubleList(int capacity) {
    super(capacity, SCHEMA);
  }

  @Override
  public double getPrimitive(int index) {
    return get(index);
  }

  @Override
  public boolean addPrimitive(double o) {
    return add(o);
  }
}
