package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * This class will apply {@link DatumWriterCustomization} at runtime.
 * @param <T>
 */
public class CustomizedSpecificDatumWriter<T> extends SpecificDatumWriter<T> {
  private final DatumWriterCustomization customization;

  public CustomizedSpecificDatumWriter(Schema schema, SpecificData modelData, DatumWriterCustomization customization) {
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
