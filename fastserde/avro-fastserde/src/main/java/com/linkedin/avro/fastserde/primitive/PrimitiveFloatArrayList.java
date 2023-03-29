package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveFloatList;
import org.apache.avro.Schema;


public class PrimitiveFloatArrayList extends PrimitiveArrayList<Float, PrimitiveFloatList, float[]> implements PrimitiveFloatList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.FLOAT));

  public PrimitiveFloatArrayList(int capacity) {
    super(capacity);
  }

  public PrimitiveFloatArrayList() {
    super();
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
  protected float[] newArray(int capacity) {
    return new float[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveFloatList that, int index) {
    return Float.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object object) {
    return object instanceof PrimitiveBooleanList;
  }
}
