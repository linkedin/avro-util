package build.generated;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.specific.SpecificData;


public enum SpamType {
  FIRST_ENUM, UNKNOWN, NEW_ENUM;

  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"enum\",\"name\":\"SpamType\",\"namespace\":\"build.generated\",\"symbols\":[\"FIRST_ENUM\", \"UNKNOWN\", \"NEW_ENUM\"],\"doc\":\"auto-generated for avro compatibility\"}");
 private static final org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();
 

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }
}
