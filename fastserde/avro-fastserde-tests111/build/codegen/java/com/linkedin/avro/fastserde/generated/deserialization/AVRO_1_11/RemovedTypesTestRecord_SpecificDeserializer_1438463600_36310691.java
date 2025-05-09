
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class RemovedTypesTestRecord_SpecificDeserializer_1438463600_36310691
    implements FastDeserializer<RemovedTypesTestRecord>
{

    private final Schema readerSchema;

    public RemovedTypesTestRecord_SpecificDeserializer_1438463600_36310691(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public RemovedTypesTestRecord deserialize(RemovedTypesTestRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeRemovedTypesTestRecord0((reuse), (decoder), (customization));
    }

    public RemovedTypesTestRecord deserializeRemovedTypesTestRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        RemovedTypesTestRecord removedTypesTestRecord0;
        if ((reuse)!= null) {
            removedTypesTestRecord0 = ((RemovedTypesTestRecord)(reuse));
        } else {
            removedTypesTestRecord0 = new RemovedTypesTestRecord();
        }
        Utf8 charSequence0;
        Object oldString0 = removedTypesTestRecord0 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        removedTypesTestRecord0 .put(0, charSequence0);
        return removedTypesTestRecord0;
    }

}
