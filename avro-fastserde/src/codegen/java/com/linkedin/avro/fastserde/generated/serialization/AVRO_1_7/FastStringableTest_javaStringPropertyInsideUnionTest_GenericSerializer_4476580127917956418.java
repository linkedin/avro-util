
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastStringableTest_javaStringPropertyInsideUnionTest_GenericSerializer_4476580127917956418
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastStringableTest_javaStringPropertyInsideUnionTest0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastStringableTest_javaStringPropertyInsideUnionTest0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
        Integer favorite_number0 = ((Integer) data.get(1));
        if (favorite_number0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (favorite_number0 instanceof Integer) {
                (encoder).writeIndex(1);
                (encoder).writeInt(((Integer) favorite_number0));
            }
        }
        CharSequence favorite_color0 = ((CharSequence) data.get(2));
        if (favorite_color0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (favorite_color0 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (favorite_color0 instanceof Utf8) {
                    (encoder).writeString(((Utf8) favorite_color0));
                } else {
                    (encoder).writeString(favorite_color0 .toString());
                }
            }
        }
    }

}
