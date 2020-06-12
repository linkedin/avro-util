package com.linkedin.avro.fastserde.primitive;

import java.util.AbstractList;
import java.util.Iterator;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;


public abstract class PrimitiveArrayList<T, L> extends AbstractList<T>
    implements GenericContainer, Comparable<GenericArray<T>> {
  private int size = 0;

  // Abstract functions required by child classes

  /**
   * @return the size of the primitive array maintained by the child class, which could be larger than {@link #size}.
   */
  protected abstract int capacity();

  /**
   * A function used to replace the primitve array instance, in cases where it needs to be resized.
   *
   * @param newElements primitive array instance to replace the current one
   */
  protected abstract void setElementsArray(Object newElements);

  /**
   * @return the primitive array instance of the child class
   */
  protected abstract Object getElementsArray();

  /**
   * @param capacity of the new primitive array
   * @return an instance of the right type of primitive array used by the child class
   */
  protected abstract Object newArray(int capacity);

  /**
   * @param that an instance of primitive list to use for comparison
   * @param index the index of the element to compare
   * @return the comparison result between element of this and {@param that} list at the provided {@param index}
   */
  protected abstract int compareElementAtIndex(L that, int index);

  /**
   * @param o instance of an Object that may or may not be a primitive list
   * @return true if {@param o} instanceof the right type of primitive list for comparison
   */
  protected abstract boolean isInstanceOfCorrectPrimitiveList(Object o);

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
    System.arraycopy(getElementsArray(), i+1, getElementsArray(), i, (size-i));
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
   * @param i an index position which may or may not be within bound of the current list
   * @throws IndexOutOfBoundsException if {@param i} is larger than the index of the last valid element
   */
  protected void checkIfLargerThanSize(int i) {
    if (i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
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
      Object newElements = newArray((size * 3)/2 + 1);
      System.arraycopy(getElementsArray(), 0, newElements, 0, size);
      setElementsArray(newElements);
    }
  }

  /** Create an empty space in the array at the index specified by {@param location} by shifting the elements to the right */
  protected void addInternal(int location) {
    if (location > size || location < 0) {
      throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
    }
    if (size == capacity()) {
      Object newElements = newArray((size * 3)/2 + 1);
      if (location > 0) {
        // Copy the elements before the insertion location, if any, with same index
        System.arraycopy(getElementsArray(), 0, newElements, 0, location - 1);
      }
      // Copy the elements after the insertion location, with index offset by 1
      System.arraycopy(getElementsArray(), location, newElements, location + 1, size - location);
      setElementsArray(newElements);
    } else {
      System.arraycopy(getElementsArray(), location, getElementsArray(), location + 1, size - location);
    }
    size++;
  }
}