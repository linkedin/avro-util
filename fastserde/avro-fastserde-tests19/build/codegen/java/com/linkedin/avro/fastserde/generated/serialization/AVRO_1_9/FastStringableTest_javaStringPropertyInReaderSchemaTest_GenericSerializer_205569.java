
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastStringableTest_javaStringPropertyInReaderSchemaTest_GenericSerializer_205569
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastStringableTest_javaStringPropertyInReaderSchemaTest0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastStringableTest_javaStringPropertyInReaderSchemaTest0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (((CharSequence) data.get(0)) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) data.get(0))));
        } else {
            (encoder).writeString(((CharSequence) data.get(0)).toString());
        }
        serialize_FastStringableTest_javaStringPropertyInReaderSchemaTest0(data, (encoder));
        serialize_FastStringableTest_javaStringPropertyInReaderSchemaTest1(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastStringableTest_javaStringPropertyInReaderSchemaTest0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence testUnionString0 = ((CharSequence) data.get(1));
        if (testUnionString0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (((CharSequence) testUnionString0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) testUnionString0)));
            } else {
                (encoder).writeString(((CharSequence) testUnionString0).toString());
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
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastStringableTest_javaStringPropertyInReaderSchemaTest1(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Map<CharSequence, CharSequence> testStringMap0 = ((Map<CharSequence, CharSequence> ) data.get(3));
        (encoder).writeMapStart();
        if ((testStringMap0 == null)||testStringMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testStringMap0 .size());
            for (CharSequence key0 : ((Map<CharSequence, CharSequence> ) testStringMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                if (((CharSequence) testStringMap0 .get(key0)) instanceof Utf8) {
                    (encoder).writeString(((Utf8)((CharSequence) testStringMap0 .get(key0))));
                } else {
                    (encoder).writeString(((CharSequence) testStringMap0 .get(key0)).toString());
                }
            }
        }
        (encoder).writeMapEnd();
    }

}
