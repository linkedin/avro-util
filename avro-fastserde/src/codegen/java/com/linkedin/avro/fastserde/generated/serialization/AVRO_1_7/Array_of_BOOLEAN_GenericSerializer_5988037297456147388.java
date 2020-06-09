
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
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
            for (int counter0 = 0; (counter0 <((List<Boolean> ) data).size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeBoolean(((Boolean) data.get(counter0)));
            }
        }
        (encoder).writeArrayEnd();
    }

}
