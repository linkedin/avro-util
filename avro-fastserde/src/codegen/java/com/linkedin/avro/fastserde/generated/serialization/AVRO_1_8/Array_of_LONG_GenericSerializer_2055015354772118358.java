
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_LONG_GenericSerializer_2055015354772118358
    implements FastSerializer<List<Long>>
{


    public void serialize(List<Long> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            if (data instanceof PrimitiveLongList) {
                PrimitiveLongList primitiveList0 = null;
                primitiveList0 = ((PrimitiveLongList) data);
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
