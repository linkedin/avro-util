
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Map_of_UNION_GenericSerializer_2087096002965517991
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
            for (CharSequence key86 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key86);
                IndexedRecord union87 = null;
                union87 = ((Map<CharSequence, IndexedRecord> ) data).get(key86);
                if (union87 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union87 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record".equals(((IndexedRecord) union87).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        serializerecord88(((IndexedRecord) union87), (encoder));
                    }
                }
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord88(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field89 = ((CharSequence) data.get(0));
        if (field89 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field89 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field89 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field89));
                } else {
                    (encoder).writeString(field89 .toString());
                }
            }
        }
    }

}
