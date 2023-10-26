
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastSerdeFixed_GenericSerializer_504108934
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        serializeFastSerdeFixed0(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeFixed0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        (encoder).writeFixed(((GenericFixed) data.get(0)).bytes());
        serialize_FastSerdeFixed0(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeFixed0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        List<GenericFixed> arrayOfFixed0 = ((List<GenericFixed> ) data.get(1));
        if (arrayOfFixed0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (arrayOfFixed0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<GenericFixed> ) arrayOfFixed0) == null)||((List<GenericFixed> ) arrayOfFixed0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<GenericFixed> ) arrayOfFixed0).size());
                    for (int counter0 = 0; (counter0 <((List<GenericFixed> ) arrayOfFixed0).size()); counter0 ++) {
                        (encoder).startItem();
                        (encoder).writeFixed(((GenericFixed)((List<GenericFixed> ) arrayOfFixed0).get(counter0)).bytes());
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, GenericFixed> mapOfFixed0 = ((Map<CharSequence, GenericFixed> ) data.get(2));
        (customization).getCheckMapTypeFunction().apply(mapOfFixed0);
        (encoder).writeMapStart();
        if ((mapOfFixed0 == null)||mapOfFixed0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(mapOfFixed0 .size());
            for (CharSequence key0 : ((Map<CharSequence, GenericFixed> ) mapOfFixed0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                (encoder).writeFixed(((GenericFixed) mapOfFixed0 .get(key0)).bytes());
            }
        }
        (encoder).writeMapEnd();
    }

}
