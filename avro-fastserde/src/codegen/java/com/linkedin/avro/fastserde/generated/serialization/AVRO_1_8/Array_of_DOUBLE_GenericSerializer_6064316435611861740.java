
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_DOUBLE_GenericSerializer_6064316435611861740
    implements FastSerializer<List<Double>>
{


    public void serialize(List<Double> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            for (int counter0 = 0; (counter0 <((List<Double> ) data).size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeDouble(((Double) data.get(counter0)));
            }
        }
        (encoder).writeArrayEnd();
    }

}
