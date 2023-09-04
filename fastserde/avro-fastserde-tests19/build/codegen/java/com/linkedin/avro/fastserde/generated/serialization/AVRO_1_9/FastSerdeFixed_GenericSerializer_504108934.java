
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastSerdeFixed_GenericSerializer_504108934
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeFixed0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeFixed0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) data.get(0)).bytes());
        serialize_FastSerdeFixed0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeFixed0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<org.apache.avro.generic.GenericData.Fixed> arrayOfFixed0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(1));
        if (arrayOfFixed0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (arrayOfFixed0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<org.apache.avro.generic.GenericData.Fixed> ) arrayOfFixed0) == null)||((List<org.apache.avro.generic.GenericData.Fixed> ) arrayOfFixed0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<org.apache.avro.generic.GenericData.Fixed> ) arrayOfFixed0).size());
                    for (int counter0 = 0; (counter0 <((List<org.apache.avro.generic.GenericData.Fixed> ) arrayOfFixed0).size()); counter0 ++) {
                        (encoder).startItem();
                        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed)((List<org.apache.avro.generic.GenericData.Fixed> ) arrayOfFixed0).get(counter0)).bytes());
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, org.apache.avro.generic.GenericData.Fixed> mapOfFixed0 = ((Map<CharSequence, org.apache.avro.generic.GenericData.Fixed> ) data.get(2));
        (encoder).writeMapStart();
        if ((mapOfFixed0 == null)||mapOfFixed0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(mapOfFixed0 .size());
            for (CharSequence key0 : ((Map<CharSequence, org.apache.avro.generic.GenericData.Fixed> ) mapOfFixed0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) mapOfFixed0 .get(key0)).bytes());
            }
        }
        (encoder).writeMapEnd();
    }

}
