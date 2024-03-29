
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Array_of_record_GenericSerializer_477614132
    implements FastSerializer<List<IndexedRecord>>
{


    public void serialize(List<IndexedRecord> data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter0 = 0; (counter0 <data.size()); counter0 ++) {
                (encoder).startItem();
                IndexedRecord record0 = null;
                record0 = ((List<IndexedRecord> ) data).get(counter0);
                serializeRecord0(record0, (encoder), (customization));
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializeRecord0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        CharSequence field0 = ((CharSequence) data.get(0));
        if (field0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (((CharSequence) field0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) field0)));
            } else {
                (encoder).writeString(((CharSequence) field0).toString());
            }
        }
    }

}
