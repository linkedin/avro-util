
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Map_of_UNION_GenericSerializer_2087096002965517991
    implements FastSerializer<Map<CharSequence, IndexedRecord>>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(Map<CharSequence, IndexedRecord> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeMapStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (CharSequence key87 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key87);
                IndexedRecord union88 = null;
                union88 = ((Map<CharSequence, IndexedRecord> ) data).get(key87);
                if (union88 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union88 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record".equals(((IndexedRecord) union88).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        serializerecord89(((IndexedRecord) union88), (encoder));
                    }
                }
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord89(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field90 = ((CharSequence) data.get(0));
        if (field90 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field90 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field90 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field90));
                } else {
                    (encoder).writeString(field90 .toString());
                }
            }
        }
    }

}
