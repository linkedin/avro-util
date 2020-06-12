package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import org.apache.avro.Schema;


public class PrimitiveBooleanArrayList extends PrimitiveArrayList<Boolean, PrimitiveBooleanList> implements PrimitiveBooleanList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.BOOLEAN));
  boolean[] elementsArray;

  public PrimitiveBooleanArrayList(int capacity) {
    this.elementsArray = new boolean[capacity];
  }

  @Override
  public Boolean get(int index) {
    return getPrimitive(index);
  }

  @Override
  public boolean getPrimitive(int index) {
    checkIfLargerThanSize(index);
    return elementsArray[index];
  }

  @Override
  public boolean add(Boolean o) {
    return addPrimitive(o);
  }

  @Override
  public boolean addPrimitive(boolean e) {
    capacityCheck();
    elementsArray[getAndIncrementSize()] = e;
    return true;
  }

  @Override
  public void add(int position, Boolean e) {
    addInternal(position);
    elementsArray[position] = e;
  }

  @Override
  public Boolean set(int index, Boolean element) {
    return setPrimitive(index, element);
  }

  @Override
  public boolean setPrimitive(int index, boolean element) {
    checkIfLargerThanSize(index);
    boolean response = elementsArray[index];
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
    this.elementsArray = (boolean[]) newElements;
  }

  @Override
  protected Object getElementsArray() {
    return this.elementsArray;
  }

  @Override
  protected Object newArray(int capacity) {
    return new boolean[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveBooleanList that, int index) {
    return Boolean.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object o) {
    return o instanceof PrimitiveBooleanList;
  }
}
