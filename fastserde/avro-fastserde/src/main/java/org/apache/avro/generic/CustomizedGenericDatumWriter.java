package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;


/**
 * This class will apply {@link DatumWriterCustomization} at runtime.
 */
public class CustomizedGenericDatumWriter<T> extends GenericDatumWriter<T> {

  private final DatumWriterCustomization customization;

  public CustomizedGenericDatumWriter(Schema schema, GenericData modelData, DatumWriterCustomization customization) {
    super(schema, modelData);

    this.customization = customization;
  }

  @Override
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    if (customization != null && customization.getCheckMapTypeFunction() != null) {
      customization.getCheckMapTypeFunction().apply(datum);
    }
    super.writeMap(schema, datum, out);
  }
}
