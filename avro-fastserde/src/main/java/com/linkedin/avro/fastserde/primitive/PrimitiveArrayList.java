package com.linkedin.avro.fastserde.primitive;

import java.util.AbstractList;
import java.util.Iterator;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;


public abstract class PrimitiveArrayList<T, L, A> extends AbstractList<T>
    implements GenericContainer, Comparable<GenericArray<T>> {
  private int size = 0;
  protected A elementsArray;

  public PrimitiveArrayList(int capacity) {
    this.elementsArray = newArray(capacity);
  }

  public PrimitiveArrayList() {
    this(10);
  }

  // Abstract functions required by child classes

  /**
   * @return the size of the primitive array maintained by the child class, which could be larger than {@link #size}.
   */
  protected abstract int capacity();

  /**
   * @param capacity of the new primitive array
   * @return an instance of the right type of primitive array used by the child class
   */
  protected abstract A newArray(int capacity);

  /**
   * @param that an instance of primitive list to use for comparison
   * @param index the index of the element to compare
   * @return the comparison result between element of this and that list at the provided index
   */
  protected abstract int compareElementAtIndex(L that, int index);

  /**
   * @param object instance of an Object that may or may not be a primitive list
   * @return true if the passed in object is an instance of the right type of primitive list for comparison
   */
  protected abstract boolean isInstanceOfCorrectPrimitiveList(Object object);

  // Public API

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    size = 0;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int position = 0;

      @Override
      public boolean hasNext() {
        return position < size;
      }

      @Override
      public T next() {
        return get(position++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public T remove(int i) {
    checkIfLargerThanSize(i);
    T result = (T) get(i);
    --size;
    System.arraycopy(elementsArray, i+1, elementsArray, i, (size-i));
    return result;
  }

  @Override
  public int compareTo(GenericArray<T> that) {
    if (isInstanceOfCorrectPrimitiveList(that)) {
      L thatPrimitiveList = (L) that;
      if (this.size == that.size()) {
        for (int i = 0; i < this.size; i++) {
          int compare = compareElementAtIndex(thatPrimitiveList, i);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      } else if (this.size > that.size()) {
        return 1;
      } else {
        return -1;
      }
    } else {
      // Not our own type of primitive list, so we will delegate to the regular implementation, which will do boxing
      return GenericData.get().compare(this, that, this.getSchema());
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    int count = 0;
    for (T e : this) {
      buffer.append(e==null ? "null" : e.toString());
      if (++count < size()) {
        buffer.append(", ");
      }
    }
    buffer.append("]");
    return buffer.toString();
  }

  // Utilities for child classes

  /**
   * A function to check if a requested index is within bound of the list. This is needed because {@link #size}
   * may be smaller than {@link #capacity()}. This can happen in the case of object reuse, where the previous
   * instance had a larger number of elements than the new one, in which case, we will reuse the array, but
   * use the {@link #size} as a boundary within the larger array.
   *
   * @param index an index position which may or may not be within bound of the current list
   * @throws IndexOutOfBoundsException if the index param is larger than the index of the last valid element
   */
  protected void checkIfLargerThanSize(int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds.");
    }
  }

  /**
   * A function used when appending an element to the end of the list. It increments the size as a side-effect.
   *
   * N.B.: Since {@link #size} is private, this is the only size mutation operation allowed for child classes.
   *
   * @return the index of the appended element
   */
  protected int getAndIncrementSize() {
    return size++;
  }

  /** Checks if the primitve array is at capacity, and if so, resizes it to 1.5x + 1. */
  protected void capacityCheck() {
    if (size == capacity()) {
      A newElements = newArray((size * 3)/2 + 1);
      System.arraycopy(elementsArray, 0, newElements, 0, size);
      this.elementsArray = newElements;
    }
  }

  /**
   * Create an empty space in the array at the index specified by shifting the elements to the right
   *
   * @param location index where the element will be added
   */
  protected void addInternal(int location) {
    if (location > size || location < 0) {
      throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
    }
    if (size == capacity()) {
      A newElements = newArray((size * 3)/2 + 1);
      if (location > 0) {
        // Copy the elements before the insertion location, if any, with same index
        System.arraycopy(elementsArray, 0, newElements, 0, location - 1);
      }
      // Copy the elements after the insertion location, with index offset by 1
      System.arraycopy(elementsArray, location, newElements, location + 1, size - location);
      this.elementsArray = newElements;
    } else {
      System.arraycopy(elementsArray, location, elementsArray, location + 1, size - location);
    }
    size++;
  }
}