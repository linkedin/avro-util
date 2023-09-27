
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class recordName_GenericDeserializer_1743054079_1743054079
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema unionField0;

    public recordName_GenericDeserializer_1743054079_1743054079(Schema readerSchema) {
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
        Utf8 charSequence0;
        Object oldString0 = recordName.get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        recordName.put(0, charSequence0);
        populate_recordName0((recordName), (decoder));
        return recordName;
    }

    private void populate_recordName0(IndexedRecord recordName, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            recordName.put(1, null);
        } else {
            if (unionIndex0 == 1) {
                recordName.put(1, deserializerecordName0(recordName.get(1), (decoder)));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionField': "+ unionIndex0));
            }
        }
    }

}
