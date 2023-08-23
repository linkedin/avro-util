
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;

public class Array_of_BOOLEAN_GenericSerializer_869749973
    implements FastSerializer<List<Boolean>>
{

    private final GenericData modelData;

    public Array_of_BOOLEAN_GenericSerializer_869749973(GenericData modelData) {
        this.modelData = modelData;
    }

    public void serialize(List<Boolean> data, Encoder encoder)
        throws IOException
    {
        (encoder).writeArrayStart();
        Object array0 = data;
        if ((data == null)||data.isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(data.size());
            if (array0 instanceof PrimitiveBooleanList) {
                PrimitiveBooleanList primitiveList0 = ((PrimitiveBooleanList) array0);
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
