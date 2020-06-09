
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_INT_GenericSerializer_3343716480540445685
    implements FastSerializer<List<Integer>>
{


    public void serialize(List<Integer> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter0 = 0; (counter0 <((List<Integer> ) data).size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeInt(((Integer) data.get(counter0)));
            }
        }
        (encoder).writeArrayEnd();
    }

}
