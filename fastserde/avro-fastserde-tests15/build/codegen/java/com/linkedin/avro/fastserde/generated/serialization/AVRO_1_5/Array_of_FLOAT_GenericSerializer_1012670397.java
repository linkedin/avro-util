
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_5;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_FLOAT_GenericSerializer_1012670397
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
            Object array0 = data;
            if (array0 instanceof PrimitiveFloatList) {
                PrimitiveFloatList primitiveList0 = ((PrimitiveFloatList) array0);
                for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                    (encoder).startItem();
                    (encoder).writeFloat(primitiveList0 .getPrimitive(counter0));
                }
            } else {
                for (int counter1 = 0; (counter1 <data.size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeFloat(data.get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
