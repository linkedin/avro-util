/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.linkedin.avroutil1.compatibility.avro16.codec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class BinaryDecoder extends Decoder {
  private BinaryDecoder.ByteSource source = null;
  private byte[] buf = null;
  private int minPos = 0;
  private int pos = 0;
  private int limit = 0;
  private final Utf8 scratchUtf8 = new Utf8();

  BinaryDecoder.BufferAccessor getBufferAccessor() {
    return new BinaryDecoder.BufferAccessor(this);
  }

  protected BinaryDecoder() {
  }

  BinaryDecoder(int bufferSize, InputStream in) {
    this.configure(bufferSize, in);
  }

  BinaryDecoder(byte[] data, int offset, int length) {
    this.configure(data, offset, length);
  }


  BinaryDecoder configure(InputStream in) {
    configureSource(8192, new BinaryDecoder.InputStreamByteSource(in));
    return this;
  }

  BinaryDecoder configure(int bufferSize, InputStream in) {
    this.configureSource(bufferSize, new BinaryDecoder.InputStreamByteSource(in));
    return this;
  }

  BinaryDecoder configure(byte[] data, int offset, int length) {
    this.configureSource(8192, new BinaryDecoder.ByteArrayByteSource(data, offset, length));
    return this;
  }

  void configureSource(int bufferSize, BinaryDecoder.ByteSource source) {
    if (null != this.source) {
      this.source.detach();
    }

    source.attach(bufferSize, this);
    this.source = source;
  }

  public void readNull() throws IOException {
  }

  public boolean readBoolean() throws IOException {
    if (this.limit == this.pos) {
      this.limit = this.source.tryReadRaw(this.buf, 0, this.buf.length);
      this.pos = 0;
      if (this.limit == 0) {
        throw new EOFException();
      }
    }

    int n = this.buf[this.pos++] & 255;
    return n == 1;
  }

  public int readInt() throws IOException {
    this.ensureBounds(5);
    int len = 1;
    int b = this.buf[this.pos] & 255;
    int n = b & 127;
    if (b > 127) {
      b = this.buf[this.pos + len++] & 255;
      n ^= (b & 127) << 7;
      if (b > 127) {
        b = this.buf[this.pos + len++] & 255;
        n ^= (b & 127) << 14;
        if (b > 127) {
          b = this.buf[this.pos + len++] & 255;
          n ^= (b & 127) << 21;
          if (b > 127) {
            b = this.buf[this.pos + len++] & 255;
            n ^= (b & 127) << 28;
            if (b > 127) {
              throw new IOException("Invalid int encoding");
            }
          }
        }
      }
    }

    this.pos += len;
    if (this.pos > this.limit) {
      throw new EOFException();
    } else {
      return n >>> 1 ^ -(n & 1);
    }
  }

  public long readLong() throws IOException {
    this.ensureBounds(10);
    int b = this.buf[this.pos++] & 255;
    int n = b & 127;
    long l;
    if (b > 127) {
      b = this.buf[this.pos++] & 255;
      n ^= (b & 127) << 7;
      if (b > 127) {
        b = this.buf[this.pos++] & 255;
        n ^= (b & 127) << 14;
        if (b > 127) {
          b = this.buf[this.pos++] & 255;
          n ^= (b & 127) << 21;
          if (b > 127) {
            l = this.innerLongDecode((long)n);
          } else {
            l = (long)n;
          }
        } else {
          l = (long)n;
        }
      } else {
        l = (long)n;
      }
    } else {
      l = (long)n;
    }

    if (this.pos > this.limit) {
      throw new EOFException();
    } else {
      return l >>> 1 ^ -(l & 1L);
    }
  }

  private long innerLongDecode(long l) throws IOException {
    int len = 1;
    int b = this.buf[this.pos] & 255;
    l ^= ((long)b & 127L) << 28;
    if (b > 127) {
      b = this.buf[this.pos + len++] & 255;
      l ^= ((long)b & 127L) << 35;
      if (b > 127) {
        b = this.buf[this.pos + len++] & 255;
        l ^= ((long)b & 127L) << 42;
        if (b > 127) {
          b = this.buf[this.pos + len++] & 255;
          l ^= ((long)b & 127L) << 49;
          if (b > 127) {
            b = this.buf[this.pos + len++] & 255;
            l ^= ((long)b & 127L) << 56;
            if (b > 127) {
              b = this.buf[this.pos + len++] & 255;
              l ^= ((long)b & 127L) << 63;
              if (b > 127) {
                throw new IOException("Invalid long encoding");
              }
            }
          }
        }
      }
    }

    this.pos += len;
    return l;
  }

  public float readFloat() throws IOException {
    this.ensureBounds(4);
    int len = 1;
    int var10000 = this.buf[this.pos] & 255;
    int var3 = len + 1;
    int n = var10000 | (this.buf[this.pos + len] & 255) << 8 | (this.buf[this.pos + var3++] & 255) << 16 | (this.buf[this.pos + var3++] & 255) << 24;
    if (this.pos + 4 > this.limit) {
      throw new EOFException();
    } else {
      this.pos += 4;
      return Float.intBitsToFloat(n);
    }
  }

  public double readDouble() throws IOException {
    this.ensureBounds(8);
    int len = 1;
    int var10000 = this.buf[this.pos] & 255;
    int var4 = len + 1;
    int n1 = var10000 | (this.buf[this.pos + len] & 255) << 8 | (this.buf[this.pos + var4++] & 255) << 16 | (this.buf[this.pos + var4++] & 255) << 24;
    int n2 = this.buf[this.pos + var4++] & 255 | (this.buf[this.pos + var4++] & 255) << 8 | (this.buf[this.pos + var4++] & 255) << 16 | (this.buf[this.pos + var4++] & 255) << 24;
    if (this.pos + 8 > this.limit) {
      throw new EOFException();
    } else {
      this.pos += 8;
      return Double.longBitsToDouble((long)n1 & 4294967295L | (long)n2 << 32);
    }
  }

  public Utf8 readString(Utf8 old) throws IOException {
    int length = this.readInt();
    Utf8 result = old != null ? old : new Utf8();
    result.setByteLength(length);
    if (0 != length) {
      this.doReadBytes(result.getBytes(), 0, length);
    }

    return result;
  }

  public String readString() throws IOException {
    return this.readString(this.scratchUtf8).toString();
  }

  public void skipString() throws IOException {
    this.doSkipBytes((long)this.readInt());
  }

  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = this.readInt();
    ByteBuffer result;
    if (old != null && length <= old.capacity()) {
      result = old;
      old.clear();
    } else {
      result = ByteBuffer.allocate(length);
    }

    this.doReadBytes(result.array(), result.position(), length);
    result.limit(length);
    return result;
  }

  public void skipBytes() throws IOException {
    this.doSkipBytes((long)this.readInt());
  }

  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    this.doReadBytes(bytes, start, length);
  }

  public void skipFixed(int length) throws IOException {
    this.doSkipBytes((long)length);
  }

  public int readEnum() throws IOException {
    return this.readInt();
  }

  protected void doSkipBytes(long length) throws IOException {
    int remaining = this.limit - this.pos;
    if (length <= (long)remaining) {
      this.pos = (int)((long)this.pos + length);
    } else {
      this.limit = this.pos = 0;
      length -= (long)remaining;
      this.source.skipSourceBytes(length);
    }

  }

  protected void doReadBytes(byte[] bytes, int start, int length) throws IOException {
    int remaining = this.limit - this.pos;
    if (length <= remaining) {
      System.arraycopy(this.buf, this.pos, bytes, start, length);
      this.pos += length;
    } else {
      System.arraycopy(this.buf, this.pos, bytes, start, remaining);
      start += remaining;
      length -= remaining;
      this.pos = this.limit;
      this.source.readRaw(bytes, start, length);
    }

  }

  protected long doReadItemCount() throws IOException {
    long result = this.readLong();
    if (result < 0L) {
      this.readLong();
      result = -result;
    }

    return result;
  }

  private long doSkipItems() throws IOException {
    long result;
    for(result = (long)this.readInt(); result < 0L; result = (long)this.readInt()) {
      long bytecount = this.readLong();
      this.doSkipBytes(bytecount);
    }

    return result;
  }

  public long readArrayStart() throws IOException {
    return this.doReadItemCount();
  }

  public long arrayNext() throws IOException {
    return this.doReadItemCount();
  }

  public long skipArray() throws IOException {
    return this.doSkipItems();
  }

  public long readMapStart() throws IOException {
    return this.doReadItemCount();
  }

  public long mapNext() throws IOException {
    return this.doReadItemCount();
  }

  public long skipMap() throws IOException {
    return this.doSkipItems();
  }

  public int readIndex() throws IOException {
    return this.readInt();
  }

  public boolean isEnd() throws IOException {
    if (this.limit - this.pos > 0) {
      return false;
    } else if (this.source.isEof()) {
      return true;
    } else {
      int read = this.source.tryReadRaw(this.buf, 0, this.buf.length);
      this.pos = 0;
      this.limit = read;
      return 0 == read;
    }
  }

  private void ensureBounds(int num) throws IOException {
    int remaining = this.limit - this.pos;
    if (remaining < num) {
      this.source.compactAndFill(this.buf, this.pos, this.minPos, remaining);
    }

  }

  public InputStream inputStream() {
    return this.source;
  }

  private static class ByteArrayByteSource extends BinaryDecoder.ByteSource {
    private byte[] data;
    private int position;
    private int max;
    private boolean compacted;

    private ByteArrayByteSource(byte[] data, int start, int len) {
      this.compacted = false;
      if (data.length >= 16 && len >= 16) {
        this.data = data;
        this.position = start;
        this.max = start + len;
      } else {
        this.data = new byte[16];
        System.arraycopy(data, start, this.data, 0, len);
        this.position = 0;
        this.max = len;
      }

    }

    protected void attach(int bufferSize, BinaryDecoder decoder) {
      decoder.buf = this.data;
      decoder.pos = this.position;
      decoder.minPos = this.position;
      decoder.limit = this.max;
      this.ba = new BinaryDecoder.BufferAccessor(decoder);
    }

    protected void skipSourceBytes(long length) throws IOException {
      long skipped = this.trySkipBytes(length);
      if (skipped < length) {
        throw new EOFException();
      }
    }

    protected long trySkipBytes(long length) throws IOException {
      this.max = this.ba.getLim();
      this.position = this.ba.getPos();
      long remaining = (long)(this.max - this.position);
      if (remaining >= length) {
        this.position = (int)((long)this.position + length);
        this.ba.setPos(this.position);
        return length;
      } else {
        this.position = (int)((long)this.position + remaining);
        this.ba.setPos(this.position);
        return remaining;
      }
    }

    protected void readRaw(byte[] data, int off, int len) throws IOException {
      int read = this.tryReadRaw(data, off, len);
      if (read < len) {
        throw new EOFException();
      }
    }

    protected int tryReadRaw(byte[] data, int off, int len) throws IOException {
      return 0;
    }

    protected void compactAndFill(byte[] buf, int pos, int minPos, int remaining) throws IOException {
      if (!this.compacted) {
        byte[] tinybuf = new byte[remaining + 16];
        System.arraycopy(buf, pos, tinybuf, 0, remaining);
        this.ba.setBuf(tinybuf, 0, remaining);
        this.compacted = true;
      }

    }

    public int read() throws IOException {
      this.max = this.ba.getLim();
      this.position = this.ba.getPos();
      if (this.position >= this.max) {
        return -1;
      } else {
        int result = this.ba.getBuf()[this.position++] & 255;
        this.ba.setPos(this.position);
        return result;
      }
    }

    public void close() throws IOException {
      this.ba.setPos(this.ba.getLim());
    }

    public boolean isEof() {
      int remaining = this.ba.getLim() - this.ba.getPos();
      return remaining == 0;
    }
  }

  private static class InputStreamByteSource extends BinaryDecoder.ByteSource {
    private InputStream in;
    protected boolean isEof;

    private InputStreamByteSource(InputStream in) {
      this.isEof = false;
      this.in = in;
    }

    protected void skipSourceBytes(long length) throws IOException {
      boolean readZero = false;

      while(length > 0L) {
        long n = this.in.skip(length);
        if (n > 0L) {
          length -= n;
        } else {
          if (n != 0L) {
            this.isEof = true;
            throw new EOFException();
          }

          if (readZero) {
            this.isEof = true;
            throw new EOFException();
          }

          readZero = true;
        }
      }

    }

    protected long trySkipBytes(long length) throws IOException {
      long leftToSkip = length;

      try {
        boolean readZero = false;

        while(leftToSkip > 0L) {
          long n = this.in.skip(length);
          if (n > 0L) {
            leftToSkip -= n;
          } else {
            if (n != 0L) {
              this.isEof = true;
              break;
            }

            if (readZero) {
              this.isEof = true;
              break;
            }

            readZero = true;
          }
        }
      } catch (EOFException var8) {
        this.isEof = true;
      }

      return length - leftToSkip;
    }

    protected void readRaw(byte[] data, int off, int len) throws IOException {
      while(len > 0) {
        int read = this.in.read(data, off, len);
        if (read < 0) {
          this.isEof = true;
          throw new EOFException();
        }

        len -= read;
        off += read;
      }

    }

    protected int tryReadRaw(byte[] data, int off, int len) throws IOException {
      int leftToCopy = len;

      try {
        while(leftToCopy > 0) {
          int read = this.in.read(data, off, leftToCopy);
          if (read < 0) {
            this.isEof = true;
            break;
          }

          leftToCopy -= read;
          off += read;
        }
      } catch (EOFException var6) {
        this.isEof = true;
      }

      return len - leftToCopy;
    }

    public int read() throws IOException {
      if (this.ba.getLim() - this.ba.getPos() == 0) {
        return this.in.read();
      } else {
        int position = this.ba.getPos();
        int result = this.ba.getBuf()[position] & 255;
        this.ba.setPos(position + 1);
        return result;
      }
    }

    public boolean isEof() {
      return this.isEof;
    }

    public void close() throws IOException {
      this.in.close();
    }
  }

  abstract static class ByteSource extends InputStream {
    protected BinaryDecoder.BufferAccessor ba;

    protected ByteSource() {
    }

    abstract boolean isEof();

    protected void attach(int bufferSize, BinaryDecoder decoder) {
      decoder.buf = new byte[bufferSize];
      decoder.pos = 0;
      decoder.minPos = 0;
      decoder.limit = 0;
      this.ba = new BinaryDecoder.BufferAccessor(decoder);
    }

    protected void detach() {
      this.ba.detach();
    }

    protected abstract void skipSourceBytes(long var1) throws IOException;

    protected abstract long trySkipBytes(long var1) throws IOException;

    protected abstract void readRaw(byte[] var1, int var2, int var3) throws IOException;

    protected abstract int tryReadRaw(byte[] var1, int var2, int var3) throws IOException;

    protected void compactAndFill(byte[] buf, int pos, int minPos, int remaining) throws IOException {
      System.arraycopy(buf, pos, buf, minPos, remaining);
      this.ba.setPos(minPos);
      int newLimit = remaining + this.tryReadRaw(buf, minPos + remaining, buf.length - remaining);
      this.ba.setLimit(newLimit);
    }

    public int read(byte[] b, int off, int len) throws IOException {
      int lim = this.ba.getLim();
      int pos = this.ba.getPos();
      byte[] buf = this.ba.getBuf();
      int remaining = lim - pos;
      if (remaining >= len) {
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        this.ba.setPos(pos);
        return len;
      } else {
        System.arraycopy(buf, pos, b, off, remaining);
        pos += remaining;
        this.ba.setPos(pos);
        int inputRead = remaining + this.tryReadRaw(b, off + remaining, len - remaining);
        return inputRead == 0 ? -1 : inputRead;
      }
    }

    public long skip(long n) throws IOException {
      int lim = this.ba.getLim();
      int pos = this.ba.getPos();
      int remaining = lim - pos;
      if ((long)remaining > n) {
        pos = (int)((long)pos + n);
        this.ba.setPos(pos);
        return n;
      } else {
        this.ba.setPos(lim);
        long isSkipCount = this.trySkipBytes(n - (long)remaining);
        return isSkipCount + (long)remaining;
      }
    }

    public int available() throws IOException {
      return this.ba.getLim() - this.ba.getPos();
    }
  }

  static class BufferAccessor {
    private final BinaryDecoder decoder;
    private byte[] buf;
    private int pos;
    private int limit;
    boolean detached;

    private BufferAccessor(BinaryDecoder decoder) {
      this.detached = false;
      this.decoder = decoder;
    }

    void detach() {
      this.buf = this.decoder.buf;
      this.pos = this.decoder.pos;
      this.limit = this.decoder.limit;
      this.detached = true;
    }

    int getPos() {
      return this.detached ? this.pos : this.decoder.pos;
    }

    int getLim() {
      return this.detached ? this.limit : this.decoder.limit;
    }

    byte[] getBuf() {
      return this.detached ? this.buf : this.decoder.buf;
    }

    void setPos(int pos) {
      if (this.detached) {
        this.pos = pos;
      } else {
        this.decoder.pos = pos;
      }

    }

    void setLimit(int limit) {
      if (this.detached) {
        this.limit = limit;
      } else {
        this.decoder.limit = limit;
      }

    }

    void setBuf(byte[] buf, int offset, int length) {
      if (this.detached) {
        this.buf = buf;
        this.limit = offset + length;
        this.pos = offset;
      } else {
        this.decoder.buf = buf;
        this.decoder.limit = offset + length;
        this.decoder.pos = offset;
        this.decoder.minPos = offset;
      }

    }
  }
}
