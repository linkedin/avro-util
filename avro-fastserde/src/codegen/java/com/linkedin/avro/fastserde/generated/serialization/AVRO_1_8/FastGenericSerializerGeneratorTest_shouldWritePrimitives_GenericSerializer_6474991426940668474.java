
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWritePrimitives_GenericSerializer_6474991426940668474
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives102(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives102(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(0)));
        Integer testIntUnion103 = ((Integer) data.get(1));
        if (testIntUnion103 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testIntUnion103 instanceof Integer) {
                (encoder).writeIndex(1);
                (encoder).writeInt(((Integer) testIntUnion103));
            }
        }
        if (data.get(2) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(2)));
        } else {
            (encoder).writeString(data.get(2).toString());
        }
        CharSequence testStringUnion104 = ((CharSequence) data.get(3));
        if (testStringUnion104 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testStringUnion104 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (testStringUnion104 instanceof Utf8) {
                    (encoder).writeString(((Utf8) testStringUnion104));
                } else {
                    (encoder).writeString(testStringUnion104 .toString());
                }
            }
        }
        (encoder).writeLong(((Long) data.get(4)));
        Long testLongUnion105 = ((Long) data.get(5));
        if (testLongUnion105 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testLongUnion105 instanceof Long) {
                (encoder).writeIndex(1);
                (encoder).writeLong(((Long) testLongUnion105));
            }
        }
        (encoder).writeDouble(((Double) data.get(6)));
        Double testDoubleUnion106 = ((Double) data.get(7));
        if (testDoubleUnion106 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testDoubleUnion106 instanceof Double) {
                (encoder).writeIndex(1);
                (encoder).writeDouble(((Double) testDoubleUnion106));
            }
        }
        (encoder).writeFloat(((Float) data.get(8)));
        Float testFloatUnion107 = ((Float) data.get(9));
        if (testFloatUnion107 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testFloatUnion107 instanceof Float) {
                (encoder).writeIndex(1);
                (encoder).writeFloat(((Float) testFloatUnion107));
            }
        }
        (encoder).writeBoolean(((Boolean) data.get(10)));
        Boolean testBooleanUnion108 = ((Boolean) data.get(11));
        if (testBooleanUnion108 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testBooleanUnion108 instanceof Boolean) {
                (encoder).writeIndex(1);
                (encoder).writeBoolean(((Boolean) testBooleanUnion108));
            }
        }
        (encoder).writeBytes(((ByteBuffer) data.get(12)));
        ByteBuffer testBytesUnion109 = ((ByteBuffer) data.get(13));
        if (testBytesUnion109 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testBytesUnion109 instanceof ByteBuffer) {
                (encoder).writeIndex(1);
                (encoder).writeBytes(((ByteBuffer) testBytesUnion109));
            }
        }
    }

}
