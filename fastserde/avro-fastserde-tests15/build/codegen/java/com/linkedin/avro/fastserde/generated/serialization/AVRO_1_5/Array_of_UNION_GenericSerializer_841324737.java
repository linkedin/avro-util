
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_5;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class Array_of_UNION_GenericSerializer_841324737
    implements FastSerializer<List<IndexedRecord>>
{


    public Array_of_UNION_GenericSerializer_841324737() {
    }

    public void serialize(List<IndexedRecord> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        Object array0 = data;
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter0 = 0; (counter0 <data.size()); counter0 ++) {
                (encoder).startItem();
                IndexedRecord union0 = null;
                union0 = ((List<IndexedRecord> ) data).get(counter0);
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
        (encoder).writeArrayEnd();
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
            if (((CharSequence) field0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) field0)));
            } else {
                (encoder).writeString(((CharSequence) field0).toString());
            }
        }
    }

}
