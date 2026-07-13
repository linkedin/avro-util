
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class OuterRecord_GenericDeserializer_998347834_1261326440
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema optionalString0;
    private final Schema innerRecord0;
    private final Schema innerRecordNameRecordSchema0;

    public OuterRecord_GenericDeserializer_998347834_1261326440(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.optionalString0 = readerSchema.getField("optionalString").schema();
        this.innerRecord0 = readerSchema.getField("innerRecord").schema();
        this.innerRecordNameRecordSchema0 = innerRecord0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeOuterRecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeOuterRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord outerRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            outerRecord0 = ((IndexedRecord)(reuse));
        } else {
            outerRecord0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            outerRecord0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = outerRecord0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                outerRecord0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'optionalString': "+ unionIndex0));
            }
        }
        populate_OuterRecord0((outerRecord0), (customization), (decoder));
        return outerRecord0;
    }

    private void populate_OuterRecord0(IndexedRecord outerRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        outerRecord0 .put(1, deserializeinnerRecordName0(outerRecord0 .get(1), (decoder), (customization)));
    }

    public IndexedRecord deserializeinnerRecordName0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord innerRecordName0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == innerRecordNameRecordSchema0)) {
            innerRecordName0 = ((IndexedRecord)(reuse));
        } else {
            innerRecordName0 = new org.apache.avro.generic.GenericData.Record(innerRecordNameRecordSchema0);
        }
        innerRecordName0 .put(0, (decoder.readInt()));
        return innerRecordName0;
    }

}
