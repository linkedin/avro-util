
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_INT_GenericSerializer_1012089072
    implements FastSerializer<List<Integer>>
{


    public Array_of_INT_GenericSerializer_1012089072() {
    }

    public void serialize(List<Integer> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        Object array0 = data;
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            if (array0 instanceof PrimitiveIntList) {
                PrimitiveIntList primitiveList0 = ((PrimitiveIntList) array0);
                for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                    (encoder).startItem();
                    (encoder).writeInt(primitiveList0 .getPrimitive(counter0));
                }
            } else {
                for (int counter1 = 0; (counter1 <data.size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeInt(data.get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
