package build.generated;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;






public class SpamUser extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 9178456496644807758L;
  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"SpamUser\",\"namespace\":\"build.generated\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"SpamType\",\"symbols\":[\"FIRST_ENUM\",\"UNKNOWN\",\"NEW_ENUM\"],\"default\":\"UNKNOWN\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();

  

   public build.generated.SpamType f;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public SpamUser() {}

  /**
   * All-args constructor.
   * @param f The new value for f
   */
  public SpamUser(build.generated.SpamType f) {
    this.f = f;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return f;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: f = (build.generated.SpamType)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'f' field.
   * @return The value of the 'f' field.
   */
  public build.generated.SpamType getF() {
    return f;
  }


  /**
   * Sets the value of the 'f' field.
   * @param value the value to set.
   */
  public void setF(build.generated.SpamType value) {
    this.f = value;
  }

  

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<SpamUser>
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter<>(SCHEMA$);

  public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, AvroCompatibilityHelper.newBinaryEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<SpamUser>
    READER$ = new org.apache.avro.specific.SpecificDatumReader<>(SCHEMA$);

  public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, AvroCompatibilityHelper.newBinaryDecoder(in));
  }

  
}










