
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveLongList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class Array_of_LONG_GenericDeserializer_2055015354772118358_2055015354772118358
    implements FastDeserializer<List<Long>>
{

    private final Schema readerSchema;

    public Array_of_LONG_GenericDeserializer_2055015354772118358_2055015354772118358(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Long> deserialize(List<Long> reuse, Decoder decoder)
        throws IOException
    {
        PrimitiveLongList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (chunkLen0 > 0) {
            if ((reuse) instanceof PrimitiveLongList) {
                array0 = ((PrimitiveLongList)(reuse));
                array0 .clear();
            } else {
                array0 = new ColdPrimitiveLongList(((int) chunkLen0));
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    array0 .addPrimitive((decoder.readLong()));
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            array0 = new ColdPrimitiveLongList(((int) chunkLen0));
        }
        return array0;
    }

}
