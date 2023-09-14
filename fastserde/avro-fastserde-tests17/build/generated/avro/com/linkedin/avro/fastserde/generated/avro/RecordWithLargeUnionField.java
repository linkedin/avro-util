/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.avro.fastserde.generated.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class RecordWithLargeUnionField extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RecordWithLargeUnionField\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"fields\":[{\"name\":\"unionField\",\"type\":[\"string\",\"int\",\"bytes\"],\"default\":\"more than 50 letters aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.Object unionField;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public RecordWithLargeUnionField() {}

  /**
   * All-args constructor.
   */
  public RecordWithLargeUnionField(java.lang.Object unionField) {
    this.unionField = unionField;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return unionField;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: unionField = (java.lang.Object)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'unionField' field.
   */
  public java.lang.Object getUnionField() {
    return unionField;
  }

  /**
   * Sets the value of the 'unionField' field.
   * @param value the value to set.
   */
  public void setUnionField(java.lang.Object value) {
    this.unionField = value;
  }

  /** Creates a new RecordWithLargeUnionField RecordBuilder */
  public static com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder newBuilder() {
    return new com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder();
  }
  
  /** Creates a new RecordWithLargeUnionField RecordBuilder by copying an existing Builder */
  public static com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder other) {
    return new com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder(other);
  }
  
  /** Creates a new RecordWithLargeUnionField RecordBuilder by copying an existing RecordWithLargeUnionField instance */
  public static com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder newBuilder(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField other) {
    return new com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder(other);
  }
  
  /**
   * RecordBuilder for RecordWithLargeUnionField instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RecordWithLargeUnionField>
    implements org.apache.avro.data.RecordBuilder<RecordWithLargeUnionField> {

    private java.lang.Object unionField;

    /** Creates a new Builder */
    private Builder() {
      super(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.unionField)) {
        this.unionField = data().deepCopy(fields()[0].schema(), other.unionField);
        fieldSetFlags()[0] = true;
      }
    }
    
    /** Creates a Builder by copying an existing RecordWithLargeUnionField instance */
    private Builder(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField other) {
            super(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.SCHEMA$);
      if (isValidValue(fields()[0], other.unionField)) {
        this.unionField = data().deepCopy(fields()[0].schema(), other.unionField);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'unionField' field */
    public java.lang.Object getUnionField() {
      return unionField;
    }
    
    /** Sets the value of the 'unionField' field */
    public com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder setUnionField(java.lang.Object value) {
      validate(fields()[0], value);
      this.unionField = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'unionField' field has been set */
    public boolean hasUnionField() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'unionField' field */
    public com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField.Builder clearUnionField() {
      unionField = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public RecordWithLargeUnionField build() {
      try {
        RecordWithLargeUnionField record = new RecordWithLargeUnionField();
        record.unionField = fieldSetFlags()[0] ? this.unionField : (java.lang.Object) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
