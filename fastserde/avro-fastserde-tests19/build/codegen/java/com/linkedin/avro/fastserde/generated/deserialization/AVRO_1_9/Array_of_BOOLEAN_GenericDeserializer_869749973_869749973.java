
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveBooleanArrayList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;

public class Array_of_BOOLEAN_GenericDeserializer_869749973_869749973
    implements FastDeserializer<List<Boolean>>
{

    private final Schema readerSchema;
    private final GenericData modelData;

    public Array_of_BOOLEAN_GenericDeserializer_869749973_869749973(Schema readerSchema, GenericData modelData) {
        this.readerSchema = readerSchema;
        this.modelData = modelData;
    }

    public List<Boolean> deserialize(List<Boolean> reuse, Decoder decoder)
        throws IOException
    {
        PrimitiveBooleanList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof PrimitiveBooleanList) {
            array0 = ((PrimitiveBooleanList)(reuse));
            array0 .clear();
        } else {
            array0 = new PrimitiveBooleanArrayList(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                array0 .addPrimitive((decoder.readBoolean()));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

}
