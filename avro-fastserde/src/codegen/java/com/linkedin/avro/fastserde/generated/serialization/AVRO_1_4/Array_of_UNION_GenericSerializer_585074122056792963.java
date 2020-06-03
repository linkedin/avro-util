
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Array_of_UNION_GenericSerializer_585074122056792963
    implements FastSerializer<List<IndexedRecord>>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(List<IndexedRecord> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter64 = 0; (counter64 <((List<IndexedRecord> ) data).size()); counter64 ++) {
                (encoder).startItem();
                IndexedRecord union65 = null;
                union65 = ((List<IndexedRecord> ) data).get(counter64);
                if (union65 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union65 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record".equals(((IndexedRecord) union65).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        serializerecord66(((IndexedRecord) union65), (encoder));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord66(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field67 = ((CharSequence) data.get(0));
        if (field67 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field67 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field67 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field67));
                } else {
                    (encoder).writeString(field67 .toString());
                }
            }
        }
    }

}
