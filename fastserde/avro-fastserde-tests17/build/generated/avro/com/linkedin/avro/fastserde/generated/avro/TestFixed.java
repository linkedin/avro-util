/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.avro.fastserde.generated.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.FixedSize(1)
@org.apache.avro.specific.AvroGenerated
public class TestFixed extends org.apache.avro.specific.SpecificFixed {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"TestFixed\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"size\":1}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  
  /** Creates a new TestFixed */
  public TestFixed() {
    super();
  }
  
  /** Creates a new TestFixed with the given bytes */
  public TestFixed(byte[] bytes) {
    super(bytes);
  }
}
