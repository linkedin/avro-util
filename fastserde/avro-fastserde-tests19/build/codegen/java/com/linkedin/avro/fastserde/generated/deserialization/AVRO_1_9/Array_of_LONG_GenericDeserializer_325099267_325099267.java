
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;

public class Array_of_LONG_GenericDeserializer_325099267_325099267
    implements FastDeserializer<List<Long>>
{

    private final Schema readerSchema;
    private final GenericData modelData;

    public Array_of_LONG_GenericDeserializer_325099267_325099267(Schema readerSchema, GenericData modelData) {
        this.readerSchema = readerSchema;
        this.modelData = modelData;
    }

    public List<Long> deserialize(List<Long> reuse, Decoder decoder)
        throws IOException
    {
        PrimitiveLongList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof PrimitiveLongList) {
            array0 = ((PrimitiveLongList)(reuse));
            array0 .clear();
        } else {
            array0 = new PrimitiveLongArrayList(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                array0 .addPrimitive((decoder.readLong()));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

}
