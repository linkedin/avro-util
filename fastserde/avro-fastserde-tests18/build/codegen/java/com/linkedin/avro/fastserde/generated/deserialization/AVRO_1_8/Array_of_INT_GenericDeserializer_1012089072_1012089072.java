
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class Array_of_INT_GenericDeserializer_1012089072_1012089072
    implements FastDeserializer<List<Integer>>
{

    private final Schema readerSchema;

    public Array_of_INT_GenericDeserializer_1012089072_1012089072(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Integer> deserialize(List<Integer> reuse, Decoder decoder)
        throws IOException
    {
        PrimitiveIntList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof PrimitiveIntList) {
            array0 = ((PrimitiveIntList)(reuse));
            array0 .clear();
        } else {
            array0 = new PrimitiveIntArrayList(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                array0 .addPrimitive((decoder.readInt()));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

}
