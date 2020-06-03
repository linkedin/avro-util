
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

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
        List<Integer> array603 = null;
        long chunkLen604 = (decoder.readArrayStart());
        if (chunkLen604 > 0) {
            List<Integer> arrayReuse605 = null;
            if ((reuse) instanceof List) {
                arrayReuse605 = ((List)(reuse));
            }
            if (arrayReuse605 != (null)) {
                arrayReuse605 .clear();
                array603 = arrayReuse605;
            } else {
                array603 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen604), readerSchema);
            }
            do {
                for (int counter606 = 0; (counter606 <chunkLen604); counter606 ++) {
                    Object arrayArrayElementReuseVar607 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar607 = ((GenericArray)(reuse)).peek();
                    }
                    array603 .add((decoder.readInt()));
                }
                chunkLen604 = (decoder.arrayNext());
            } while (chunkLen604 > 0);
        } else {
            array603 = new org.apache.avro.generic.GenericData.Array<Integer>(0, readerSchema);
        }
        return array603;
    }

}
