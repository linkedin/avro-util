
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
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
        List<Boolean> array592 = null;
        long chunkLen593 = (decoder.readArrayStart());
        if (chunkLen593 > 0) {
            List<Boolean> arrayReuse594 = null;
            if ((reuse) instanceof List) {
                arrayReuse594 = ((List)(reuse));
            }
            if (arrayReuse594 != (null)) {
                arrayReuse594 .clear();
                array592 = arrayReuse594;
            } else {
                array592 = new org.apache.avro.generic.GenericData.Array<Boolean>(((int) chunkLen593), readerSchema);
            }
            do {
                for (int counter595 = 0; (counter595 <chunkLen593); counter595 ++) {
                    Object arrayArrayElementReuseVar596 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar596 = ((GenericArray)(reuse)).peek();
                    }
                    array592 .add((decoder.readBoolean()));
                }
                chunkLen593 = (decoder.arrayNext());
            } while (chunkLen593 > 0);
        } else {
            array592 = new org.apache.avro.generic.GenericData.Array<Boolean>(0, readerSchema);
        }
        return array592;
    }

}
