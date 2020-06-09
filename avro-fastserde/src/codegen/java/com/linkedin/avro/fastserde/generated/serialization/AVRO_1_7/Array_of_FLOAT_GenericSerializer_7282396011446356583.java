
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_FLOAT_GenericSerializer_7282396011446356583
    implements FastSerializer<List<Float>>
{


    public void serialize(List<Float> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter0 = 0; (counter0 <((List<Float> ) data).size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeFloat(((Float) data.get(counter0)));
            }
        }
        (encoder).writeArrayEnd();
    }

}
