
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_6;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWritePrimitives_GenericSerializer_538850553
    implements FastSerializer<IndexedRecord>
{


    public FastGenericSerializerGeneratorTest_shouldWritePrimitives_GenericSerializer_538850553() {
    }

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(0)));
        Integer testIntUnion0 = ((Integer) data.get(1));
        if (testIntUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeInt(((Integer) testIntUnion0));
        }
        Integer testFlippedIntUnion0 = ((Integer) data.get(2));
        if (testFlippedIntUnion0 == null) {
            (encoder).writeIndex(1);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(0);
            (encoder).writeInt(((Integer) testFlippedIntUnion0));
        }
        if (((CharSequence) data.get(3)) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) data.get(3))));
        } else {
            (encoder).writeString(((CharSequence) data.get(3)).toString());
        }
        CharSequence testStringUnion0 = ((CharSequence) data.get(4));
        if (testStringUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (((CharSequence) testStringUnion0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) testStringUnion0)));
            } else {
                (encoder).writeString(((CharSequence) testStringUnion0).toString());
            }
        }
        (encoder).writeLong(((Long) data.get(5)));
        Long testLongUnion0 = ((Long) data.get(6));
        if (testLongUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeLong(((Long) testLongUnion0));
        }
        (encoder).writeDouble(((Double) data.get(7)));
        Double testDoubleUnion0 = ((Double) data.get(8));
        if (testDoubleUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeDouble(((Double) testDoubleUnion0));
        }
        (encoder).writeFloat(((Float) data.get(9)));
        Float testFloatUnion0 = ((Float) data.get(10));
        if (testFloatUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeFloat(((Float) testFloatUnion0));
        }
        (encoder).writeBoolean(((Boolean) data.get(11)));
        Boolean testBooleanUnion0 = ((Boolean) data.get(12));
        if (testBooleanUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeBoolean(((Boolean) testBooleanUnion0));
        }
        (encoder).writeBytes(((ByteBuffer) data.get(13)));
        ByteBuffer testBytesUnion0 = ((ByteBuffer) data.get(14));
        if (testBytesUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeBytes(((ByteBuffer) testBytesUnion0));
        }
    }

}
