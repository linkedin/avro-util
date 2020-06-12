package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveFloatList;
import org.apache.avro.Schema;


public class PrimitiveFloatArrayList extends PrimitiveArrayList<Float, PrimitiveFloatList> implements PrimitiveFloatList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.FLOAT));
  float[] elementsArray;

  public PrimitiveFloatArrayList(int capacity) {
    this.elementsArray = new float[capacity];
  }

  @Override
  public Float get(int index) {
    return getPrimitive(index);
  }

  @Override
  public float getPrimitive(int index) {
    checkIfLargerThanSize(index);
    return elementsArray[index];
  }

  @Override
  public boolean add(Float o) {
    return addPrimitive(o);
  }

  @Override
  public boolean addPrimitive(float e) {
    capacityCheck();
    elementsArray[getAndIncrementSize()] = e;
    return true;
  }

  @Override
  public void add(int position, Float e) {
    addInternal(position);
    elementsArray[position] = e;
  }

  @Override
  public Float set(int index, Float element) {
    return setPrimitive(index, element);
  }

  @Override
  public float setPrimitive(int index, float element) {
    checkIfLargerThanSize(index);
    float response = elementsArray[index];
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
    this.elementsArray = (float[]) newElements;
  }

  @Override
  protected Object getElementsArray() {
    return this.elementsArray;
  }

  @Override
  protected Object newArray(int capacity) {
    return new float[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveFloatList that, int index) {
    return Float.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object o) {
    return o instanceof PrimitiveBooleanList;
  }
}
