
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
        List<Integer> array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (chunkLen0 > 0) {
            List<Integer> arrayReuse0 = null;
            if ((reuse) instanceof List) {
                arrayReuse0 = ((List)(reuse));
            }
            if (arrayReuse0 != (null)) {
                arrayReuse0 .clear();
                array0 = arrayReuse0;
            } else {
                array0 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen0), readerSchema);
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Object arrayArrayElementReuseVar0 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                    }
                    array0 .add((decoder.readInt()));
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            array0 = new org.apache.avro.generic.GenericData.Array<Integer>(0, readerSchema);
        }
        return array0;
    }

}
