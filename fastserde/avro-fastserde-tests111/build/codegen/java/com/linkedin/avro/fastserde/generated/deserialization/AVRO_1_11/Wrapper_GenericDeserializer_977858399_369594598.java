
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Wrapper_GenericDeserializer_977858399_369594598
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema metadata0;
    private final Schema metadataOptionSchema0;
    private final Schema fieldName0;

    public Wrapper_GenericDeserializer_977858399_369594598(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.metadata0 = readerSchema.getField("metadata").schema();
        this.metadataOptionSchema0 = metadata0 .getTypes().get(1);
        this.fieldName0 = metadataOptionSchema0 .getField("fieldName").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeWrapper0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeWrapper0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord wrapper0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            wrapper0 = ((IndexedRecord)(reuse));
        } else {
            wrapper0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            wrapper0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                wrapper0 .put(0, deserializemetadata0(wrapper0 .get(0), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'metadata': "+ unionIndex0));
            }
        }
        return wrapper0;
    }

    public IndexedRecord deserializemetadata0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord metadata1;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == metadataOptionSchema0)) {
            metadata1 = ((IndexedRecord)(reuse));
        } else {
            metadata1 = new org.apache.avro.generic.GenericData.Record(metadataOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            metadata1 .put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence0;
                Object oldString0 = metadata1 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                metadata1 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'fieldName': "+ unionIndex1));
            }
        }
        return metadata1;
    }

}
