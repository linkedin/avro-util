
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class Array_of_INT_GenericDeserializer_3343716480540445685_3343716480540445685
    implements FastDeserializer<List<Integer>>
{

    private final Schema readerSchema;

    public Array_of_INT_GenericDeserializer_3343716480540445685_3343716480540445685(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Integer> deserialize(List<Integer> reuse, Decoder decoder)
        throws IOException
    {
        List<Integer> array749 = null;
        long chunkLen750 = (decoder.readArrayStart());
        if (chunkLen750 > 0) {
            List<Integer> arrayReuse751 = null;
            if ((reuse) instanceof List) {
                arrayReuse751 = ((List)(reuse));
            }
            if (arrayReuse751 != (null)) {
                arrayReuse751 .clear();
                array749 = arrayReuse751;
            } else {
                array749 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen750), readerSchema);
            }
            do {
                for (int counter752 = 0; (counter752 <chunkLen750); counter752 ++) {
                    Object arrayArrayElementReuseVar753 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar753 = ((GenericArray)(reuse)).peek();
                    }
                    array749 .add((decoder.readInt()));
                }
                chunkLen750 = (decoder.arrayNext());
            } while (chunkLen750 > 0);
        } else {
            array749 = new org.apache.avro.generic.GenericData.Array<Integer>(0, readerSchema);
        }
        return array749;
    }

}
