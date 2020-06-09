
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
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
            for (int counter0 = 0; (counter0 <((List<Long> ) data).size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeLong(((Long) data.get(counter0)));
            }
        }
        (encoder).writeArrayEnd();
    }

}
