
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastSerdeLogicalTypesDefined_GenericSerializer_229156053
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeLogicalTypesDefined0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeLogicalTypesDefined0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(0)));
        serialize_FastSerdeLogicalTypesDefined0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesDefined0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(1)));
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = ((List<Object> ) data.get(2));
        (encoder).writeArrayStart();
        if ((arrayOfUnionOfDateAndTimestampMillis0 == null)||arrayOfUnionOfDateAndTimestampMillis0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(arrayOfUnionOfDateAndTimestampMillis0 .size());
            for (int counter0 = 0; (counter0 <arrayOfUnionOfDateAndTimestampMillis0 .size()); counter0 ++) {
                (encoder).startItem();
                Object union0 = null;
                union0 = ((List<Object> ) arrayOfUnionOfDateAndTimestampMillis0).get(counter0);
                if (union0 instanceof Integer) {
                    (encoder).writeIndex(0);
                    (encoder).writeInt(((Integer) union0));
                } else {
                    if (union0 instanceof Long) {
                        (encoder).writeIndex(1);
                        (encoder).writeLong(((Long) union0));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
