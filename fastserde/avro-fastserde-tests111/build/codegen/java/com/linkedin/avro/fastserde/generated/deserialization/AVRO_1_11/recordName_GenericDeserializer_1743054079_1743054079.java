
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
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

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecordName0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecordName0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord recordName0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            recordName0 = ((IndexedRecord)(reuse));
        } else {
            recordName0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Utf8 charSequence0;
        Object oldString0 = recordName0 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        recordName0 .put(0, charSequence0);
        populate_recordName0((recordName0), (customization), (decoder));
        return recordName0;
    }

    private void populate_recordName0(IndexedRecord recordName0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            recordName0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                recordName0 .put(1, deserializerecordName0(recordName0 .get(1), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionField': "+ unionIndex0));
            }
        }
    }

}
