
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveDoubleList;
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
            if (data instanceof PrimitiveDoubleList) {
                PrimitiveDoubleList primitiveList0 = null;
                primitiveList0 = ((PrimitiveDoubleList) data);
                for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                    (encoder).startItem();
                    (encoder).writeDouble(primitiveList0 .getPrimitive(counter0));
                }
            } else {
                for (int counter1 = 0; (counter1 <data.size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeDouble(data.get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
