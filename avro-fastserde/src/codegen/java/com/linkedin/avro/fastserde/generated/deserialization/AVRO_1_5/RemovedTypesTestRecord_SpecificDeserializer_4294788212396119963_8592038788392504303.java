
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_5;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class RemovedTypesTestRecord_SpecificDeserializer_4294788212396119963_8592038788392504303
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord>
{

    private final Schema readerSchema;

    public RemovedTypesTestRecord_SpecificDeserializer_4294788212396119963_8592038788392504303(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord deserialize(com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeRemovedTypesTestRecord0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord deserializeRemovedTypesTestRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord RemovedTypesTestRecord;
        if ((reuse)!= null) {
            RemovedTypesTestRecord = ((com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord)(reuse));
        } else {
            RemovedTypesTestRecord = new com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord();
        }
        Object oldString0 = RemovedTypesTestRecord.get(0);
        if (oldString0 instanceof Utf8) {
            RemovedTypesTestRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            RemovedTypesTestRecord.put(0, (decoder).readString(null));
        }
        return RemovedTypesTestRecord;
    }

}
