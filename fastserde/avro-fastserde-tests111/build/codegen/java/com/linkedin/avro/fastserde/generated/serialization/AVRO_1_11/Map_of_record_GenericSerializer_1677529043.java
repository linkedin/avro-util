
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Map_of_record_GenericSerializer_1677529043
    implements FastSerializer<Map<CharSequence, IndexedRecord>>
{


    public void serialize(Map<CharSequence, IndexedRecord> data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        (customization).getCheckMapTypeFunction().apply(data);
        (encoder).writeMapStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (CharSequence key0 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                IndexedRecord record0 = null;
                record0 = ((Map<CharSequence, IndexedRecord> ) data).get(key0);
                serializeRecord0(record0, (encoder), (customization));
            }
        }
        (encoder).writeMapEnd();
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
