
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_10;

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
            for (CharSequence key0 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                IndexedRecord union0 = null;
                union0 = ((Map<CharSequence, IndexedRecord> ) data).get(key0);
                if (union0 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union0 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record".equals(((IndexedRecord) union0).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        serializeRecord0(((IndexedRecord) union0), (encoder));
                    }
                }
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializeRecord0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field0 = ((CharSequence) data.get(0));
        if (field0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (field0 instanceof Utf8) {
                (encoder).writeString(((Utf8) field0));
            } else {
                (encoder).writeString(field0 .toString());
            }
        }
    }

}
