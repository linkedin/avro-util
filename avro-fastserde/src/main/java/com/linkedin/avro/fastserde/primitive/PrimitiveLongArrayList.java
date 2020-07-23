package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveLongList;
import org.apache.avro.Schema;


public class PrimitiveLongArrayList extends PrimitiveArrayList<Long, PrimitiveLongList, long[]> implements PrimitiveLongList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.LONG));

  public PrimitiveLongArrayList(int capacity) {
    super(capacity);
  }

  public PrimitiveLongArrayList() {
    super();
  }

  @Override
  public Long get(int index) {
    return getPrimitive(index);
  }

  @Override
  public long getPrimitive(int index) {
    checkIfLargerThanSize(index);
    return elementsArray[index];
  }

  @Override
  public boolean add(Long o) {
    return addPrimitive(o);
  }

  @Override
  public boolean addPrimitive(long e) {
    capacityCheck();
    elementsArray[getAndIncrementSize()] = e;
    return true;
  }

  @Override
  public void add(int position, Long e) {
    addInternal(position);
    elementsArray[position] = e;
  }

  @Override
  public Long set(int index, Long element) {
    return setPrimitive(index, element);
  }

  @Override
  public long setPrimitive(int index, long element) {
    checkIfLargerThanSize(index);
    long response = elementsArray[index];
    elementsArray[index] = element;
    return response;
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  protected int capacity() {
    return elementsArray.length;
  }

  @Override
  protected long[] newArray(int capacity) {
    return new long[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveLongList that, int index) {
    return Long.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object object) {
    return object instanceof PrimitiveBooleanList;
  }
}
