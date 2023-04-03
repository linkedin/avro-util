package com.linkedin.avro.fastserde.primitive;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import org.apache.avro.Schema;


public class PrimitiveDoubleArrayList extends PrimitiveArrayList<Double, PrimitiveDoubleList, double[]> implements PrimitiveDoubleList {
  public static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.DOUBLE));

  public PrimitiveDoubleArrayList(int capacity) {
    super(capacity);
  }

  public PrimitiveDoubleArrayList() {
    super();
  }

  @Override
  public Double get(int index) {
    return getPrimitive(index);
  }

  @Override
  public double getPrimitive(int index) {
    checkIfLargerThanSize(index);
    return elementsArray[index];
  }

  @Override
  public boolean add(Double o) {
    return addPrimitive(o);
  }

  @Override
  public boolean addPrimitive(double e) {
    capacityCheck();
    elementsArray[getAndIncrementSize()] = e;
    return true;
  }

  @Override
  public void add(int position, Double e) {
    addInternal(position);
    elementsArray[position] = e;
  }

  @Override
  public Double set(int index, Double element) {
    return setPrimitive(index, element);
  }

  @Override
  public double setPrimitive(int index, double element) {
    checkIfLargerThanSize(index);
    double response = elementsArray[index];
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
  protected double[] newArray(int capacity) {
    return new double[capacity];
  }

  @Override
  protected int compareElementAtIndex(PrimitiveDoubleList that, int index) {
    return Double.compare(elementsArray[index], that.getPrimitive(index));
  }

  @Override
  protected boolean isInstanceOfCorrectPrimitiveList(Object object) {
    return object instanceof PrimitiveBooleanList;
  }
}
