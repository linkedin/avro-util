package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveFloatList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.commons.lang3.ArrayUtils;


/**
 * This is a re-implementation of Avro's {@link GenericData.Array} class.

 * Compared to the Avro implementation, it offers the following GC-related optimizations:
 * - It does not, by default, box primitive floats into Object Floats, though it will still do so if the
 *   regular functions are called (e.g.: {@link #get(int)}, for compatibility purposes. In order to avoid
 *   boxing, the {@link #getPrimitive(int)} function can be used instead.

 * - It does not maintain a reference to a {@link Schema} instance, since that schema would always be the
 *   same. Instead, it defines a static {@link #SCHEMA} which is used by all instances.

 * - It re-implements {@link #compareTo(GenericArray)}, {@link #equals(Object)} and {@link #hashCode()}
 *   in order to leverage the primitive types, rather than causing unintended boxing.

 *   Using VarHandle(JDK9+ API) to speed up float-array deserialization: We allocate byte array to store the raw bytes
 *   from BinaryDecoder and deserialize them only during array element access. We cache the results into the elements
 *   array after the first get access of the array so that sub-sequent array access are fast.
 *   TODO: Provide arrays for other primitive types.
 */
public class BufferBackedPrimitiveFloatList extends AbstractList<Float>
    implements GenericArray<Float>, Comparable<GenericArray<Float>>, PrimitiveFloatList {
  private static final float[] EMPTY = new float[0];
  private static final int FLOAT_SIZE = Float.BYTES;
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema SCHEMA = Schema.createArray(FLOAT_SCHEMA);
  private int size;
  private float[] elements = EMPTY;
  private boolean isCached = false;
  private byte[] byteBuffer;
  private boolean changed = false;

  private static final VarHandle VH = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.LITTLE_ENDIAN);

  public BufferBackedPrimitiveFloatList(int capacity) {
    if (capacity != 0) {
      elements = new float[capacity];
    }
  }

  public BufferBackedPrimitiveFloatList(Collection<Float> c) {
    if (c != null) {
      elements = new float[c.size()];
      addAll(c);
    }
  }

  /**
   * For testing purpose.
    */
  public void copyInternalState(BufferBackedPrimitiveFloatList another) {
    another.size = this.size;
    another.elements = this.elements;
    another.isCached = this.isCached;
    another.byteBuffer = this.byteBuffer;
    another.changed = this.changed;
  }

  /**
   * Instantiate (or re-use) and populate a {@link BufferBackedPrimitiveFloatList} from a {@link Decoder}.

   * N.B.: the caller must ensure the data is of the appropriate type by calling {@link #isFloatArray(Schema)}.
   *
   * @param old old {@link BufferBackedPrimitiveFloatList} to reuse
   * @param in {@link Decoder} to read new list from
   * @return a {@link BufferBackedPrimitiveFloatList} with data, possibly the old argument reused
   * @throws IOException on io errors
   */
  public static Object readPrimitiveFloatArray(Object old, Decoder in) throws IOException {
    long length = in.readArrayStart();
    long totalLength = 0;

    if (length > 0) {
      BufferBackedPrimitiveFloatList array = (BufferBackedPrimitiveFloatList) newPrimitiveFloatArray(old);

      do {
        long byteSize = length * FLOAT_SIZE;
        byte[] buffer = new byte[(int)byteSize];
        in.readFixed(buffer, 0, (int)byteSize);
        totalLength += length;
        length = in.arrayNext();
        /*
        This implies memory copy when there is more than one float array
        However, another option will be to keep the list of byte arrays
        and track positions similar to {@link CompositeByteBuffer}
         */
        array.byteBuffer = merge(array.byteBuffer, buffer);
      } while (length > 0);

      array.size = (int) totalLength;
      return array;
    } else {
      return new BufferBackedPrimitiveFloatList(0);
    }
  }

  private static byte[] merge(byte[] a, byte[] b) {
    if (a == null) {
      return b;
    } else {
      return ArrayUtils.addAll(a, b);
    }
  }

  /**
   *  The primitive float array `elements` will only be used when the interface user calls a mutating operation.
   *  eg add/remove else for read-only use case this will not be called.
   * @param list
   * @param totalSize
   */
  private static void setupElements(BufferBackedPrimitiveFloatList list, int totalSize) {
    if (list.elements.length != 0) {
      if (totalSize <= list.getCapacity()) {
        // reuse the float array directly
        list.clear();
      } else {
        list.resizeAndClear(totalSize);
      }
      list.size = totalSize;
      return;
    }
    list.elements = new float[totalSize];
    list.size = totalSize;
  }

  /**
     * @param expected {@link Schema} to inspect
     * @return true if the {@code expected} SCHEMA is of the right type to decode as a {@link BufferBackedPrimitiveFloatList}
     *         false otherwise
     */
  public static boolean isFloatArray(Schema expected) {
    return expected != null && Schema.Type.ARRAY.equals(expected.getType()) && FLOAT_SCHEMA.equals(
        expected.getElementType());
  }

  private static Object newPrimitiveFloatArray(Object old) {
    if (old instanceof BufferBackedPrimitiveFloatList) {
      BufferBackedPrimitiveFloatList oldFloatList = (BufferBackedPrimitiveFloatList) old;
      oldFloatList.byteBuffer = null;
      oldFloatList.isCached = false;
      oldFloatList.size = 0;
      oldFloatList.changed = false;
      return oldFloatList;
    } else {
      // Just a place holder, will set up the elements later.
      return new BufferBackedPrimitiveFloatList(0);
    }
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    size = 0;
  }

  private int getCapacity() {
    return elements.length;
  }

  private void resizeAndClear(int newSize) {
    elements = new float[newSize];
    clear();
  }

  @Override
  public Iterator<Float> iterator() {
    return new Iterator<Float>() {
      private int position = 0;

      @Override
      public boolean hasNext() {
        return position < size;
      }

      @Override
      public Float next() {
        float f = getPrimitive(position);
        position++;
        return f;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public float getPrimitive(int i) {
    if (i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    }
    if (isCached) {
      return elements[i];
    }
    return (float)VH.get(byteBuffer, i * FLOAT_SIZE);
  }

  @Override
  public Float get(int i) {
    return getPrimitive(i);
  }

  /**
   * Add a primitive float inside the list, without boxing.

   * TODO: verify if it's enough to simply store the data as primitives in order to benefit from the GC optimization.
   * @param o new float to add
   * @return true?
   */
  public boolean addPrimitive(float o) {
    cacheFromByteBuffer();
    if (size == elements.length) {
      float[] newElements = new float[(size * 3) / 2 + 1];
      System.arraycopy(elements, 0, newElements, 0, size);
      elements = newElements;
    }
    elements[size++] = o;
    changed = true;
    return true;
  }

  @Override
  public boolean add(Float o) {
    return addPrimitive(o);
  }

  @Override
  public void add(int location, Float o) {
    if (location > size || location < 0) {
      throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
    }
    cacheFromByteBuffer();
    if (size == elements.length) {
      float[] newElements = new float[(size * 3) / 2 + 1];
      System.arraycopy(elements, 0, newElements, 0, size);
      elements = newElements;
    }
    System.arraycopy(elements, location, elements, location + 1, size - location);
    elements[location] = o;
    size++;
    changed = true;
  }

  @Override
  public Float set(int i, Float o) {
    return setPrimitive(i, o);
  }

  @Override
  public float setPrimitive(int i, float o) {
    if (i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    }
    cacheFromByteBuffer();
    float response = elements[i];
    elements[i] = o;
    changed = true;

    return response;
  }

  @Override
  public Float remove(int i) {
    if (i >= size) {
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    }
    cacheFromByteBuffer();
    Float result = elements[i];
    --size;
    System.arraycopy(elements, i + 1, elements, i, (size - i));
    elements[size] = 0;
    changed = true;
    return result;
  }

  private void cacheFromByteBuffer() {
    if (isCached) {
      return;
    }
    synchronized (this) {
      if (!isCached) {
        setupElements(this, this.size);
        for (int i = 0; i < size; i++) {
          elements[i] = (float)VH.get(byteBuffer, i * FLOAT_SIZE);
        }
        isCached = true;
      }
    }
  }

  public float peekPrimitive() {
    cacheFromByteBuffer();
    return (size < elements.length) ? elements[size] : null;
  }

  @Override
  public Float peek() {
    return peekPrimitive();
  }

  @Override
  public int compareTo(GenericArray<Float> that) {
    cacheFromByteBuffer();
    if (that instanceof BufferBackedPrimitiveFloatList) {
      BufferBackedPrimitiveFloatList thatPrimitiveList = (BufferBackedPrimitiveFloatList) that;
      if (this.size == thatPrimitiveList.size) {
        for (int i = 0; i < this.size; i++) {
          int compare = Float.compare(this.elements[i], thatPrimitiveList.elements[i]);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      } else if (this.size > thatPrimitiveList.size) {
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
  public void reverse() {
    cacheFromByteBuffer();
    int left = 0;
    int right = elements.length - 1;

    while (left < right) {
      float tmp = elements[left];
      elements[left] = elements[right];
      elements[right] = tmp;

      left++;
      right--;
    }
    changed = true;
  }

  protected void writeFloatsByBackedBytes(Encoder encoder) throws IOException {
    encoder.writeFixed(byteBuffer);
  }

  public void writeFloats(Encoder encoder) throws IOException {
    if (changed) {
      /**
       * The backed {@link #byteBuffer} diverges from the current array, so this function will write float from
       * {@link #elements}.
       */
      for (int i = 0; i < size; ++i) {
        encoder.startItem();
        encoder.writeFloat(elements[i]);
      }
    } else {
      /**
       * So we will write the original bytes directly.
       */
      writeFloatsByBackedBytes(encoder);
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    int count = 0;
    for (int i = 0; i < size; i++) {
      buffer.append(getPrimitive(i));
      if (++count < size()) {
        buffer.append(", ");
      }
    }
    buffer.append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    cacheFromByteBuffer();
    if (o instanceof GenericArray) {
      return compareTo((GenericArray) o) == 0;
    } else {
      return super.equals(o);
    }
  }

  @Override
  public int hashCode() {
    cacheFromByteBuffer();
    int hashCode = 1;
    for (int i = 0; i < this.size; i++) {
      hashCode = 31 * hashCode + Float.hashCode(elements[i]);
    }
    return hashCode;
  }
}
