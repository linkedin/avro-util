
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.io.Encoder;

public class Array_of_LONG_GenericSerializer_325099267
    implements FastSerializer<List<Long>>
{


    public void serialize(List<Long> data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            Object array0 = data;
            if (array0 instanceof PrimitiveLongList) {
                PrimitiveLongList primitiveList0 = ((PrimitiveLongList) array0);
                for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                    (encoder).startItem();
                    (encoder).writeLong(primitiveList0 .getPrimitive(counter0));
                }
            } else {
                for (int counter1 = 0; (counter1 <data.size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeLong(data.get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
