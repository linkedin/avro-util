
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveBooleanList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class Array_of_BOOLEAN_GenericDeserializer_5988037297456147388_5988037297456147388
    implements FastDeserializer<List<Boolean>>
{

    private final Schema readerSchema;

    public Array_of_BOOLEAN_GenericDeserializer_5988037297456147388_5988037297456147388(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Boolean> deserialize(List<Boolean> reuse, Decoder decoder)
        throws IOException
    {
        PrimitiveBooleanList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (chunkLen0 > 0) {
            if ((reuse) instanceof PrimitiveBooleanList) {
                array0 = ((PrimitiveBooleanList)(reuse));
                array0 .clear();
            } else {
                array0 = new ColdPrimitiveBooleanList(((int) chunkLen0));
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    array0 .addPrimitive((decoder.readBoolean()));
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            array0 = new ColdPrimitiveBooleanList(((int) chunkLen0));
        }
        return array0;
    }

}
