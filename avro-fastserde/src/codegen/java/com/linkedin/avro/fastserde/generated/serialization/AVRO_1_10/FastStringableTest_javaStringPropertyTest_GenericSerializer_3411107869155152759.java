
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_10;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastStringableTest_javaStringPropertyTest_GenericSerializer_3411107869155152759
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastStringableTest_javaStringPropertyTest0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastStringableTest_javaStringPropertyTest0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
        CharSequence testUnionString0 = ((CharSequence) data.get(1));
        if (testUnionString0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (testUnionString0 instanceof Utf8) {
                (encoder).writeString(((Utf8) testUnionString0));
            } else {
                (encoder).writeString(testUnionString0 .toString());
            }
        }
        List<CharSequence> testStringArray0 = ((List<CharSequence> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testStringArray0 == null)||testStringArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testStringArray0 .size());
            for (int counter0 = 0; (counter0 <testStringArray0 .size()); counter0 ++) {
                (encoder).startItem();
                if (testStringArray0 .get(counter0) instanceof Utf8) {
                    (encoder).writeString(((Utf8) testStringArray0 .get(counter0)));
                } else {
                    (encoder).writeString(testStringArray0 .get(counter0).toString());
                }
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, CharSequence> testStringMap0 = ((Map<CharSequence, CharSequence> ) data.get(3));
        (encoder).writeMapStart();
        if ((testStringMap0 == null)||testStringMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testStringMap0 .size());
            for (CharSequence key0 : ((Map<CharSequence, CharSequence> ) testStringMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                if (testStringMap0 .get(key0) instanceof Utf8) {
                    (encoder).writeString(((Utf8) testStringMap0 .get(key0)));
                } else {
                    (encoder).writeString(testStringMap0 .get(key0).toString());
                }
            }
        }
        (encoder).writeMapEnd();
    }

}
