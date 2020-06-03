
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Map_of_record_GenericSerializer_2141121767969292399
    implements FastSerializer<Map<CharSequence, IndexedRecord>>
{


    public void serialize(Map<CharSequence, IndexedRecord> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeMapStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (CharSequence key82 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key82);
                IndexedRecord record83 = null;
                record83 = ((Map<CharSequence, IndexedRecord> ) data).get(key82);
                serializerecord84(record83, (encoder));
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord84(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field85 = ((CharSequence) data.get(0));
        if (field85 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field85 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field85 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field85));
                } else {
                    (encoder).writeString(field85 .toString());
                }
            }
        }
    }

}
