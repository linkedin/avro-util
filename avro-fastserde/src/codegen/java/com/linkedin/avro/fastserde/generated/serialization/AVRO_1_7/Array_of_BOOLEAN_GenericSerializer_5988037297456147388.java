
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.io.Encoder;

public class Array_of_BOOLEAN_GenericSerializer_5988037297456147388
    implements FastSerializer<List<Boolean>>
{


    public void serialize(List<Boolean> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            if (data instanceof PrimitiveBooleanList) {
                PrimitiveBooleanList primitiveList0 = null;
                primitiveList0 = ((PrimitiveBooleanList) data);
                for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                    (encoder).startItem();
                    (encoder).writeBoolean(primitiveList0 .getPrimitive(counter0));
                }
            } else {
                for (int counter1 = 0; (counter1 <data.size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeBoolean(data.get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
