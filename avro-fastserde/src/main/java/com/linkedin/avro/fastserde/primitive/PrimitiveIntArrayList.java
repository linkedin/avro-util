package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveIntList;
import org.apache.avro.Schema;


public class PrimitiveIntArrayList extends PrimitiveArrayList<Integer, PrimitiveIntList> implements PrimitiveIntList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.INT));
  int[] elementsArray;

  public PrimitiveIntArrayList(int capacity) {
    this.elementsArray = new int[capacity];
  }

  @Override
  public Integer get(int index) {
    return getPrimitive(index);
  }

  @Override
  public int getPrimitive(int index) {
    checkIfLargerThanSize(index);
    return elementsArray[index];
  }

  @Override
  public boolean add(Integer o) {
    return addPrimitive(o);
  }

  @Override
  public boolean addPrimitive(int e) {
    capacityCheck();
    elementsArray[getAndIncrementSize()] = e;
    return true;
  }

  @Override
  public void add(int position, Integer e) {
    addInternal(position);
    elementsArray[position] = e;
  }

  @Override
  public Integer set(int index, Integer element) {
    return setPrimitive(index, element);
  }

  @Override
  public int setPrimitive(int index, int element) {
    checkIfLargerThanSize(index);
    int response = elementsArray[index];
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
  protected void setElementsArray(Object newElements) {
    this.elementsArray = (int[]) newElements;
  }

  @Override
  protected Object getElementsArray() {
    return this.elementsArray;
  }

  @Override
  protected Object newArray(int capacity) {
    return new int[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveIntList that, int index) {
    return Integer.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object o) {
    return o instanceof PrimitiveBooleanList;
  }
}
