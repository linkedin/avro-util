/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.avro.fastserde.generated.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class StringableSubRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -4983273945131680714L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"StringableSubRecord\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"fields\":[{\"name\":\"uriField\",\"type\":{\"type\":\"string\",\"java-class\":\"java.net.URI\"}},{\"name\":\"nullStringIntUnion\",\"type\":[\"null\",\"string\",\"int\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<StringableSubRecord> ENCODER =
      new BinaryMessageEncoder<StringableSubRecord>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<StringableSubRecord> DECODER =
      new BinaryMessageDecoder<StringableSubRecord>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<StringableSubRecord> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<StringableSubRecord> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<StringableSubRecord> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<StringableSubRecord>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this StringableSubRecord to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a StringableSubRecord from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a StringableSubRecord instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static StringableSubRecord fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.net.URI uriField;
   private java.lang.Object nullStringIntUnion;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public StringableSubRecord() {}

  /**
   * All-args constructor.
   * @param uriField The new value for uriField
   * @param nullStringIntUnion The new value for nullStringIntUnion
   */
  public StringableSubRecord(java.net.URI uriField, java.lang.Object nullStringIntUnion) {
    this.uriField = uriField;
    this.nullStringIntUnion = nullStringIntUnion;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return uriField;
    case 1: return nullStringIntUnion;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: uriField = (java.net.URI)value$; break;
    case 1: nullStringIntUnion = value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'uriField' field.
   * @return The value of the 'uriField' field.
   */
  public java.net.URI getUriField() {
    return uriField;
  }


  /**
   * Sets the value of the 'uriField' field.
   * @param value the value to set.
   */
  public void setUriField(java.net.URI value) {
    this.uriField = value;
  }

  /**
   * Gets the value of the 'nullStringIntUnion' field.
   * @return The value of the 'nullStringIntUnion' field.
   */
  public java.lang.Object getNullStringIntUnion() {
    return nullStringIntUnion;
  }


  /**
   * Sets the value of the 'nullStringIntUnion' field.
   * @param value the value to set.
   */
  public void setNullStringIntUnion(java.lang.Object value) {
    this.nullStringIntUnion = value;
  }

  /**
   * Creates a new StringableSubRecord RecordBuilder.
   * @return A new StringableSubRecord RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder newBuilder() {
    return new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder();
  }

  /**
   * Creates a new StringableSubRecord RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new StringableSubRecord RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder other) {
    if (other == null) {
      return new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder();
    } else {
      return new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder(other);
    }
  }

  /**
   * Creates a new StringableSubRecord RecordBuilder by copying an existing StringableSubRecord instance.
   * @param other The existing instance to copy.
   * @return A new StringableSubRecord RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.StringableSubRecord other) {
    if (other == null) {
      return new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder();
    } else {
      return new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder(other);
    }
  }

  /**
   * RecordBuilder for StringableSubRecord instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<StringableSubRecord>
    implements org.apache.avro.data.RecordBuilder<StringableSubRecord> {

    private java.net.URI uriField;
    private java.lang.Object nullStringIntUnion;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.uriField)) {
        this.uriField = data().deepCopy(fields()[0].schema(), other.uriField);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.nullStringIntUnion)) {
        this.nullStringIntUnion = data().deepCopy(fields()[1].schema(), other.nullStringIntUnion);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing StringableSubRecord instance
     * @param other The existing instance to copy.
     */
    private Builder(com.linkedin.avro.fastserde.generated.avro.StringableSubRecord other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.uriField)) {
        this.uriField = data().deepCopy(fields()[0].schema(), other.uriField);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nullStringIntUnion)) {
        this.nullStringIntUnion = data().deepCopy(fields()[1].schema(), other.nullStringIntUnion);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'uriField' field.
      * @return The value.
      */
    public java.net.URI getUriField() {
      return uriField;
    }


    /**
      * Sets the value of the 'uriField' field.
      * @param value The value of 'uriField'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder setUriField(java.net.URI value) {
      validate(fields()[0], value);
      this.uriField = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'uriField' field has been set.
      * @return True if the 'uriField' field has been set, false otherwise.
      */
    public boolean hasUriField() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'uriField' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder clearUriField() {
      uriField = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'nullStringIntUnion' field.
      * @return The value.
      */
    public java.lang.Object getNullStringIntUnion() {
      return nullStringIntUnion;
    }


    /**
      * Sets the value of the 'nullStringIntUnion' field.
      * @param value The value of 'nullStringIntUnion'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder setNullStringIntUnion(java.lang.Object value) {
      validate(fields()[1], value);
      this.nullStringIntUnion = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'nullStringIntUnion' field has been set.
      * @return True if the 'nullStringIntUnion' field has been set, false otherwise.
      */
    public boolean hasNullStringIntUnion() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'nullStringIntUnion' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord.Builder clearNullStringIntUnion() {
      nullStringIntUnion = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StringableSubRecord build() {
      try {
        StringableSubRecord record = new StringableSubRecord();
        record.uriField = fieldSetFlags()[0] ? this.uriField : (java.net.URI) defaultValue(fields()[0]);
        record.nullStringIntUnion = fieldSetFlags()[1] ? this.nullStringIntUnion :  defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<StringableSubRecord>
    WRITER$ = (org.apache.avro.io.DatumWriter<StringableSubRecord>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<StringableSubRecord>
    READER$ = (org.apache.avro.io.DatumReader<StringableSubRecord>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










