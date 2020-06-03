
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Map_of_record_GenericSerializer_2141121767969292399
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
            for (CharSequence key83 : ((Map<CharSequence, IndexedRecord> ) data).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key83);
                IndexedRecord record84 = null;
                record84 = ((Map<CharSequence, IndexedRecord> ) data).get(key83);
                serializerecord85(record84, (encoder));
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord85(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field86 = ((CharSequence) data.get(0));
        if (field86 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field86 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field86 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field86));
                } else {
                    (encoder).writeString(field86 .toString());
                }
            }
        }
    }

}
