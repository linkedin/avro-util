/*
 * Copyright 2025 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface that enables custom decoding of
 * {@link com.linkedin.avroutil1.compatibility.backports.SpecificRecordBaseExt} instances.
 */
public interface CustomDecoder {

    /** Returns the actual order in which the reader's fields will be
     * returned to the reader.
     * Throws a runtime exception if we're not just about to read the
     * field of a record.  Also, this method will consume the field
     * information, and thus may only be called once before reading
     * the field value.  (However, if the client knows the order of
     * incoming fields, then the client does not need to call this
     * method but rather can just start reading the field values.)
     *
     * @throws AvroTypeException If we're not starting a new record
     *
     */
    Schema.Field[] readFieldOrder() throws IOException;

    /**
     * Skips the Symbol.String and returns the size of the string to copy.
     */
    int readStringSize() throws IOException;

    /**
     * Skips the Symbol.BYTES and returns the size of the bytes to copy
     */
    int readBytesSize() throws IOException;

    /** 
     * Reads fixed sized String object
     */
    void readStringData(byte[] bytes, int start, int len) throws IOException;

    /** 
     * Reads fixed sized Byte object
     */
    void readBytesData(byte[] bytes, int start, int len) throws IOException;

    /**
     * "Reads" a null value.  (Doesn't actually read anything, but
     * advances the state of the parser if the implementation is
     * stateful.)
     *  @throws AvroTypeException If this is a stateful reader and
     *          null is not the type of the next value to be read
     */
    void readNull() throws IOException;

    /**
     * Reads a boolean value written by {@link Encoder#writeBoolean}.
     * @throws AvroTypeException If this is a stateful reader and
     * boolean is not the type of the next value to be read
     */

    boolean readBoolean() throws IOException;

    /**
     * Reads an integer written by {@link Encoder#writeInt}.
     * @throws AvroTypeException If encoded value is larger than
     *          32-bits
     * @throws AvroTypeException If this is a stateful reader and
     *          int is not the type of the next value to be read
     */
    int readInt() throws IOException;

    /**
     * Reads a long written by {@link Encoder#writeLong}.
     * @throws AvroTypeException If this is a stateful reader and
     *          long is not the type of the next value to be read
     */
    long readLong() throws IOException;

    /**
     * Reads a float written by {@link Encoder#writeFloat}.
     * @throws AvroTypeException If this is a stateful reader and
     * is not the type of the next value to be read
     */
    float readFloat() throws IOException;

    /**
     * Reads a double written by {@link Encoder#writeDouble}.
     * @throws AvroTypeException If this is a stateful reader and
     *           is not the type of the next value to be read
     */
    double readDouble() throws IOException;

    /**
     * Reads a char-string written by {@link Encoder#writeString}.
     * @throws AvroTypeException If this is a stateful reader and
     * char-string is not the type of the next value to be read
     */
    Utf8 readString(Utf8 old) throws IOException;

    /**
     * Discards a char-string written by {@link Encoder#writeString}.
     *  @throws AvroTypeException If this is a stateful reader and
     *          char-string is not the type of the next value to be read
     */
    void skipString() throws IOException;

    /**
     * Reads a byte-string written by {@link Encoder#writeBytes}.
     * if old is not null and has sufficient capacity to take in
     * the bytes being read, the bytes are returned in old.
     * @throws AvroTypeException If this is a stateful reader and
     *          byte-string is not the type of the next value to be read
     */
    ByteBuffer readBytes(ByteBuffer old) throws IOException;

    /**
     * Discards a byte-string written by {@link Encoder#writeBytes}.
     *  @throws AvroTypeException If this is a stateful reader and
     *          byte-string is not the type of the next value to be read
     */
    void skipBytes() throws IOException;

    /**
     * Reads fixed sized binary object.
     * @param bytes The buffer to store the contents being read.
     * @param start The position where the data needs to be written.
     * @param length  The size of the binary object.
     * @throws AvroTypeException If this is a stateful reader and
     *          fixed sized binary object is not the type of the next
     *          value to be read or the length is incorrect.
     * @throws IOException
     */
    void readFixed(byte[] bytes, int start, int length)
            throws IOException;

    /**
     * A shorthand for readFixed(bytes, 0, bytes.length).
     * @throws AvroTypeException If this is a stateful reader and
     *          fixed sized binary object is not the type of the next
     *          value to be read or the length is incorrect.
     * @throws IOException
     */
    void readFixed(byte[] bytes) throws IOException;

    /**
     * Discards fixed sized binary object.
     * @param length  The size of the binary object to be skipped.
     * @throws AvroTypeException If this is a stateful reader and
     *          fixed sized binary object is not the type of the next
     *          value to be read or the length is incorrect.
     * @throws IOException
     */
    void skipFixed(int length) throws IOException;

    /**
     * Reads an enumeration.
     * @return The enumeration's value.
     * @throws AvroTypeException If this is a stateful reader and
     *          enumeration is not the type of the next value to be read.
     * @throws IOException
     */
    int readEnum() throws IOException;

    /**
     * Reads and returns the size of the first block of an array.  If
     * this method returns non-zero, then the caller should read the
     * indicated number of items, and then call {@link
     * #arrayNext} to find out the number of items in the next
     * block.  The typical pattern for consuming an array looks like:
     * <pre>{@code
     *   for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
     *     for (long j = 0; j < i; j++) {
     *       read next element of the array;
     *     }
     *   }
     * }</pre>
     *  @throws AvroTypeException If this is a stateful reader and
     *          array is not the type of the next value to be read */
    long readArrayStart() throws IOException;

    /**
     * Processes the next block of an array andreturns the number of items in
     * the block and let's the caller
     * read those items.
     * @throws AvroTypeException When called outside of an
     *         array context
     */
    long arrayNext() throws IOException;

    /**
     * Used for quickly skipping through an array.  Note you can
     * either skip the entire array, or read the entire array (with
     * {@link #readArrayStart}), but you can't mix the two on the
     * same array.
     *
     * This method will skip through as many items as it can, all of
     * them if possible.  It will return zero if there are no more
     * items to skip through, or an item count if it needs the client's
     * help in skipping.  The typical usage pattern is:
     * <pre>{@code
     *   for(long i = in.skipArray(); i != 0; i = i.skipArray()) {
     *     for (long j = 0; j < i; j++) {
     *       read and discard the next element of the array;
     *     }
     *   }
     * }</pre>
     * Note that this method can automatically skip through items if a
     * byte-count is found in the underlying data, or if a schema has
     * been provided to the implementation, but
     * otherwise the client will have to skip through items itself.
     *
     *  @throws AvroTypeException If this is a stateful reader and
     *          array is not the type of the next value to be read
     */
    long skipArray() throws IOException;

    /**
     * Reads and returns the size of the next block of map-entries.
     * Similar to {@link #readArrayStart}.
     *
     *  As an example, let's say you want to read a map of records,
     *  the record consisting of an Long field and a Boolean field.
     *  Your code would look something like this:
     * <pre>{@code
     *   Map<String,Record> m = new HashMap<String,Record>();
     *   Record reuse = new Record();
     *   for(long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
     *     for (long j = 0; j < i; j++) {
     *       String key = in.readString();
     *       reuse.intField = in.readInt();
     *       reuse.boolField = in.readBoolean();
     *       m.put(key, reuse);
     *     }
     *   }
     * }</pre>
     * @throws AvroTypeException If this is a stateful reader and
     *         map is not the type of the next value to be read
     */
    long readMapStart() throws IOException;

    /**
     * Processes the next block of map entries and returns the count of them.
     * Similar to {@link #arrayNext}.  See {@link #readMapStart} for details.
     * @throws AvroTypeException When called outside of a
     *         map context
     */
    long mapNext() throws IOException;

    /**
     * Support for quickly skipping through a map similar to {@link #skipArray}.
     *
     * As an example, let's say you want to skip a map of records,
     * the record consisting of an Long field and a Boolean field.
     * Your code would look something like this:
     * <pre>{@code
     *   for(long i = in.skipMap(); i != 0; i = in.skipMap()) {
     *     for (long j = 0; j < i; j++) {
     *       in.skipString();  // Discard key
     *       in.readInt(); // Discard int-field of value
     *       in.readBoolean(); // Discard boolean-field of value
     *     }
     *   }
     * }</pre>
     *  @throws AvroTypeException If this is a stateful reader and
     *          array is not the type of the next value to be read */

    long skipMap() throws IOException;

    /**
     * Reads the tag of a union written by {@link Encoder#writeIndex}.
     * @throws AvroTypeException If this is a stateful reader and
     *         union is not the type of the next value to be read
     */
    int readIndex() throws IOException;
}
