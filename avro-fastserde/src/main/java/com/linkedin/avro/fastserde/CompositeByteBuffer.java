package com.linkedin.avro.fastserde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CompositeByteBuffer {
  private int byteBufferCount;
  private List<ByteBuffer> byteBuffers;

  public CompositeByteBuffer(boolean createEmpty) {
    byteBuffers = createEmpty ? Collections.emptyList() : new ArrayList<>(2);
  }

  public ByteBuffer allocate(int index, int size) {
    ByteBuffer byteBuffer;

    // Check if we can reuse the old record's byteBuffers, else allocate a new one.
    if (byteBuffers.size() > index && byteBuffers.get(index).capacity() >= size) {
      byteBuffer = byteBuffers.get(index);
      byteBuffer.clear();
    } else {
      byteBuffer = ByteBuffer.allocate((int)size).order(ByteOrder.LITTLE_ENDIAN);
    }
    if (index < byteBuffers.size()) {
      byteBuffers.set(index, byteBuffer);
    } else {
      byteBuffers.add(byteBuffer);
    }
    return byteBuffer;
  }

  public void clear() {
    for (ByteBuffer byteBuffer : byteBuffers)  {
      byteBuffer.clear();
    }
  }

  public void setByteBufferCount(int count) {
    byteBufferCount = count;
  }

  public void setArray(float[] array) {
    int k = 0;
    for (int i = 0; i < byteBufferCount; i++) {
      ByteBuffer byteBuffer = byteBuffers.get(i);
      for (int j = 0; j < byteBuffer.limit(); j += Float.BYTES) {
        array[k++] = byteBuffer.getFloat(j);
      }
    }
  }
}
