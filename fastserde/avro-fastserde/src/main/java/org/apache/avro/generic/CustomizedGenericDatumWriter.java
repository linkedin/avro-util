package org.apache.avro.generic;

import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;


/**
 * This class will apply {@link DatumWriterCustomization} at runtime.
 */
public class CustomizedGenericDatumWriter<T> extends GenericDatumWriter<T> {

  private final DatumWriterCustomization customization;

  public CustomizedGenericDatumWriter(Schema schema, GenericData modelData, DatumWriterCustomization customization) {
    super(schema, modelData);
    if (customization == null) {
      throw new IllegalArgumentException("'customization' param should not null when constructing " +  this.getClass().getName());
    }

    this.customization = customization;
  }

  @Override
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    customization.getCheckMapTypeFunction().apply(datum);
    super.writeMap(schema, datum, out);
  }
}
