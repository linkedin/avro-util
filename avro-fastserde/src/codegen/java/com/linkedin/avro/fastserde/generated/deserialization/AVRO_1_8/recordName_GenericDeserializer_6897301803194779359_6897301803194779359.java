
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class recordName_GenericDeserializer_6897301803194779359_6897301803194779359
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema unionField0;

    public recordName_GenericDeserializer_6897301803194779359_6897301803194779359(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.unionField0 = readerSchema.getField("unionField").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializerecordName0((reuse), (decoder));
    }

    public IndexedRecord deserializerecordName0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord recordName;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            recordName = ((IndexedRecord)(reuse));
        } else {
            recordName = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        if (recordName.get(0) instanceof Utf8) {
            recordName.put(0, (decoder).readString(((Utf8) recordName.get(0))));
        } else {
            recordName.put(0, (decoder).readString(null));
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                recordName.put(1, deserializerecordName0(recordName.get(1), (decoder)));
            }
        }
        return recordName;
    }

}
