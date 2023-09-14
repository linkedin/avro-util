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
public class LocalTimestampRecordWithDefaults extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 5645968482834400203L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"LocalTimestampRecordWithDefaults\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"fields\":[{\"name\":\"nestedTimestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"},\"default\":99},{\"name\":\"nullableNestedTimestamp\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}],\"default\":null},{\"name\":\"nullableUnionOfDateAndLocalTimestamp\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"},{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}],\"default\":null},{\"name\":\"unionOfDateAndLocalTimestamp\",\"type\":[{\"type\":\"int\",\"logicalType\":\"date\"},{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}],\"default\":45678}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();
  static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion());
  }

  private static final BinaryMessageEncoder<LocalTimestampRecordWithDefaults> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<LocalTimestampRecordWithDefaults> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<LocalTimestampRecordWithDefaults> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<LocalTimestampRecordWithDefaults> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<LocalTimestampRecordWithDefaults> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this LocalTimestampRecordWithDefaults to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a LocalTimestampRecordWithDefaults from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a LocalTimestampRecordWithDefaults instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static LocalTimestampRecordWithDefaults fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.time.LocalDateTime nestedTimestamp;
  private java.time.LocalDateTime nullableNestedTimestamp;
  private java.lang.Object nullableUnionOfDateAndLocalTimestamp;
  private java.lang.Object unionOfDateAndLocalTimestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public LocalTimestampRecordWithDefaults() {}

  /**
   * All-args constructor.
   * @param nestedTimestamp The new value for nestedTimestamp
   * @param nullableNestedTimestamp The new value for nullableNestedTimestamp
   * @param nullableUnionOfDateAndLocalTimestamp The new value for nullableUnionOfDateAndLocalTimestamp
   * @param unionOfDateAndLocalTimestamp The new value for unionOfDateAndLocalTimestamp
   */
  public LocalTimestampRecordWithDefaults(java.time.LocalDateTime nestedTimestamp, java.time.LocalDateTime nullableNestedTimestamp, java.lang.Object nullableUnionOfDateAndLocalTimestamp, java.lang.Object unionOfDateAndLocalTimestamp) {
    this.nestedTimestamp = nestedTimestamp;
    this.nullableNestedTimestamp = nullableNestedTimestamp;
    this.nullableUnionOfDateAndLocalTimestamp = nullableUnionOfDateAndLocalTimestamp;
    this.unionOfDateAndLocalTimestamp = unionOfDateAndLocalTimestamp;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return nestedTimestamp;
    case 1: return nullableNestedTimestamp;
    case 2: return nullableUnionOfDateAndLocalTimestamp;
    case 3: return unionOfDateAndLocalTimestamp;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      new org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion(),
      null,
      null,
      null,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: nestedTimestamp = (java.time.LocalDateTime)value$; break;
    case 1: nullableNestedTimestamp = (java.time.LocalDateTime)value$; break;
    case 2: nullableUnionOfDateAndLocalTimestamp = value$; break;
    case 3: unionOfDateAndLocalTimestamp = value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'nestedTimestamp' field.
   * @return The value of the 'nestedTimestamp' field.
   */
  public java.time.LocalDateTime getNestedTimestamp() {
    return nestedTimestamp;
  }


  /**
   * Sets the value of the 'nestedTimestamp' field.
   * @param value the value to set.
   */
  public void setNestedTimestamp(java.time.LocalDateTime value) {
    this.nestedTimestamp = value;
  }

  /**
   * Gets the value of the 'nullableNestedTimestamp' field.
   * @return The value of the 'nullableNestedTimestamp' field.
   */
  public java.time.LocalDateTime getNullableNestedTimestamp() {
    return nullableNestedTimestamp;
  }


  /**
   * Sets the value of the 'nullableNestedTimestamp' field.
   * @param value the value to set.
   */
  public void setNullableNestedTimestamp(java.time.LocalDateTime value) {
    this.nullableNestedTimestamp = value;
  }

  /**
   * Gets the value of the 'nullableUnionOfDateAndLocalTimestamp' field.
   * @return The value of the 'nullableUnionOfDateAndLocalTimestamp' field.
   */
  public java.lang.Object getNullableUnionOfDateAndLocalTimestamp() {
    return nullableUnionOfDateAndLocalTimestamp;
  }


  /**
   * Sets the value of the 'nullableUnionOfDateAndLocalTimestamp' field.
   * @param value the value to set.
   */
  public void setNullableUnionOfDateAndLocalTimestamp(java.lang.Object value) {
    this.nullableUnionOfDateAndLocalTimestamp = value;
  }

  /**
   * Gets the value of the 'unionOfDateAndLocalTimestamp' field.
   * @return The value of the 'unionOfDateAndLocalTimestamp' field.
   */
  public java.lang.Object getUnionOfDateAndLocalTimestamp() {
    return unionOfDateAndLocalTimestamp;
  }


  /**
   * Sets the value of the 'unionOfDateAndLocalTimestamp' field.
   * @param value the value to set.
   */
  public void setUnionOfDateAndLocalTimestamp(java.lang.Object value) {
    this.unionOfDateAndLocalTimestamp = value;
  }

  /**
   * Creates a new LocalTimestampRecordWithDefaults RecordBuilder.
   * @return A new LocalTimestampRecordWithDefaults RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder newBuilder() {
    return new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder();
  }

  /**
   * Creates a new LocalTimestampRecordWithDefaults RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new LocalTimestampRecordWithDefaults RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder other) {
    if (other == null) {
      return new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder();
    } else {
      return new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder(other);
    }
  }

  /**
   * Creates a new LocalTimestampRecordWithDefaults RecordBuilder by copying an existing LocalTimestampRecordWithDefaults instance.
   * @param other The existing instance to copy.
   * @return A new LocalTimestampRecordWithDefaults RecordBuilder
   */
  public static com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults other) {
    if (other == null) {
      return new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder();
    } else {
      return new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder(other);
    }
  }

  /**
   * RecordBuilder for LocalTimestampRecordWithDefaults instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<LocalTimestampRecordWithDefaults>
    implements org.apache.avro.data.RecordBuilder<LocalTimestampRecordWithDefaults> {

    private java.time.LocalDateTime nestedTimestamp;
    private java.time.LocalDateTime nullableNestedTimestamp;
    private java.lang.Object nullableUnionOfDateAndLocalTimestamp;
    private java.lang.Object unionOfDateAndLocalTimestamp;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.nestedTimestamp)) {
        this.nestedTimestamp = data().deepCopy(fields()[0].schema(), other.nestedTimestamp);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.nullableNestedTimestamp)) {
        this.nullableNestedTimestamp = data().deepCopy(fields()[1].schema(), other.nullableNestedTimestamp);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.nullableUnionOfDateAndLocalTimestamp)) {
        this.nullableUnionOfDateAndLocalTimestamp = data().deepCopy(fields()[2].schema(), other.nullableUnionOfDateAndLocalTimestamp);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.unionOfDateAndLocalTimestamp)) {
        this.unionOfDateAndLocalTimestamp = data().deepCopy(fields()[3].schema(), other.unionOfDateAndLocalTimestamp);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing LocalTimestampRecordWithDefaults instance
     * @param other The existing instance to copy.
     */
    private Builder(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.nestedTimestamp)) {
        this.nestedTimestamp = data().deepCopy(fields()[0].schema(), other.nestedTimestamp);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nullableNestedTimestamp)) {
        this.nullableNestedTimestamp = data().deepCopy(fields()[1].schema(), other.nullableNestedTimestamp);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.nullableUnionOfDateAndLocalTimestamp)) {
        this.nullableUnionOfDateAndLocalTimestamp = data().deepCopy(fields()[2].schema(), other.nullableUnionOfDateAndLocalTimestamp);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.unionOfDateAndLocalTimestamp)) {
        this.unionOfDateAndLocalTimestamp = data().deepCopy(fields()[3].schema(), other.unionOfDateAndLocalTimestamp);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'nestedTimestamp' field.
      * @return The value.
      */
    public java.time.LocalDateTime getNestedTimestamp() {
      return nestedTimestamp;
    }


    /**
      * Sets the value of the 'nestedTimestamp' field.
      * @param value The value of 'nestedTimestamp'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder setNestedTimestamp(java.time.LocalDateTime value) {
      validate(fields()[0], value);
      this.nestedTimestamp = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'nestedTimestamp' field has been set.
      * @return True if the 'nestedTimestamp' field has been set, false otherwise.
      */
    public boolean hasNestedTimestamp() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'nestedTimestamp' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder clearNestedTimestamp() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'nullableNestedTimestamp' field.
      * @return The value.
      */
    public java.time.LocalDateTime getNullableNestedTimestamp() {
      return nullableNestedTimestamp;
    }


    /**
      * Sets the value of the 'nullableNestedTimestamp' field.
      * @param value The value of 'nullableNestedTimestamp'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder setNullableNestedTimestamp(java.time.LocalDateTime value) {
      validate(fields()[1], value);
      this.nullableNestedTimestamp = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'nullableNestedTimestamp' field has been set.
      * @return True if the 'nullableNestedTimestamp' field has been set, false otherwise.
      */
    public boolean hasNullableNestedTimestamp() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'nullableNestedTimestamp' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder clearNullableNestedTimestamp() {
      nullableNestedTimestamp = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'nullableUnionOfDateAndLocalTimestamp' field.
      * @return The value.
      */
    public java.lang.Object getNullableUnionOfDateAndLocalTimestamp() {
      return nullableUnionOfDateAndLocalTimestamp;
    }


    /**
      * Sets the value of the 'nullableUnionOfDateAndLocalTimestamp' field.
      * @param value The value of 'nullableUnionOfDateAndLocalTimestamp'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder setNullableUnionOfDateAndLocalTimestamp(java.lang.Object value) {
      validate(fields()[2], value);
      this.nullableUnionOfDateAndLocalTimestamp = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'nullableUnionOfDateAndLocalTimestamp' field has been set.
      * @return True if the 'nullableUnionOfDateAndLocalTimestamp' field has been set, false otherwise.
      */
    public boolean hasNullableUnionOfDateAndLocalTimestamp() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'nullableUnionOfDateAndLocalTimestamp' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder clearNullableUnionOfDateAndLocalTimestamp() {
      nullableUnionOfDateAndLocalTimestamp = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'unionOfDateAndLocalTimestamp' field.
      * @return The value.
      */
    public java.lang.Object getUnionOfDateAndLocalTimestamp() {
      return unionOfDateAndLocalTimestamp;
    }


    /**
      * Sets the value of the 'unionOfDateAndLocalTimestamp' field.
      * @param value The value of 'unionOfDateAndLocalTimestamp'.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder setUnionOfDateAndLocalTimestamp(java.lang.Object value) {
      validate(fields()[3], value);
      this.unionOfDateAndLocalTimestamp = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'unionOfDateAndLocalTimestamp' field has been set.
      * @return True if the 'unionOfDateAndLocalTimestamp' field has been set, false otherwise.
      */
    public boolean hasUnionOfDateAndLocalTimestamp() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'unionOfDateAndLocalTimestamp' field.
      * @return This builder.
      */
    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults.Builder clearUnionOfDateAndLocalTimestamp() {
      unionOfDateAndLocalTimestamp = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LocalTimestampRecordWithDefaults build() {
      try {
        LocalTimestampRecordWithDefaults record = new LocalTimestampRecordWithDefaults();
        record.nestedTimestamp = fieldSetFlags()[0] ? this.nestedTimestamp : (java.time.LocalDateTime) defaultValue(fields()[0]);
        record.nullableNestedTimestamp = fieldSetFlags()[1] ? this.nullableNestedTimestamp : (java.time.LocalDateTime) defaultValue(fields()[1]);
        record.nullableUnionOfDateAndLocalTimestamp = fieldSetFlags()[2] ? this.nullableUnionOfDateAndLocalTimestamp :  defaultValue(fields()[2]);
        record.unionOfDateAndLocalTimestamp = fieldSetFlags()[3] ? this.unionOfDateAndLocalTimestamp :  defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<LocalTimestampRecordWithDefaults>
    WRITER$ = (org.apache.avro.io.DatumWriter<LocalTimestampRecordWithDefaults>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<LocalTimestampRecordWithDefaults>
    READER$ = (org.apache.avro.io.DatumReader<LocalTimestampRecordWithDefaults>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










