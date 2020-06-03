
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

public class Array_of_record_GenericSerializer_1629046702287533603
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
            for (int counter60 = 0; (counter60 <((List<IndexedRecord> ) data).size()); counter60 ++) {
                (encoder).startItem();
                IndexedRecord record61 = null;
                record61 = ((List<IndexedRecord> ) data).get(counter60);
                serializerecord62(record61, (encoder));
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    public void serializerecord62(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence field63 = ((CharSequence) data.get(0));
        if (field63 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field63 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field63 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field63));
                } else {
                    (encoder).writeString(field63 .toString());
                }
            }
        }
    }

}
