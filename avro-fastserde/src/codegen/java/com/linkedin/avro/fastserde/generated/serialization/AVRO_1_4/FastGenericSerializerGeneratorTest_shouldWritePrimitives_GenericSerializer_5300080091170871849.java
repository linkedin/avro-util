
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWritePrimitives_GenericSerializer_5300080091170871849
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives103(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWritePrimitives103(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(0)));
        Integer testIntUnion104 = ((Integer) data.get(1));
        if (testIntUnion104 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testIntUnion104 instanceof Integer) {
                (encoder).writeIndex(1);
                (encoder).writeInt(((Integer) testIntUnion104));
            }
        }
        if (data.get(2) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(2)));
        } else {
            (encoder).writeString(data.get(2).toString());
        }
        CharSequence testStringUnion105 = ((CharSequence) data.get(3));
        if (testStringUnion105 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testStringUnion105 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (testStringUnion105 instanceof Utf8) {
                    (encoder).writeString(((Utf8) testStringUnion105));
                } else {
                    (encoder).writeString(testStringUnion105 .toString());
                }
            }
        }
        (encoder).writeLong(((Long) data.get(4)));
        Long testLongUnion106 = ((Long) data.get(5));
        if (testLongUnion106 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testLongUnion106 instanceof Long) {
                (encoder).writeIndex(1);
                (encoder).writeLong(((Long) testLongUnion106));
            }
        }
        (encoder).writeDouble(((Double) data.get(6)));
        Double testDoubleUnion107 = ((Double) data.get(7));
        if (testDoubleUnion107 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testDoubleUnion107 instanceof Double) {
                (encoder).writeIndex(1);
                (encoder).writeDouble(((Double) testDoubleUnion107));
            }
        }
        (encoder).writeFloat(((Float) data.get(8)));
        Float testFloatUnion108 = ((Float) data.get(9));
        if (testFloatUnion108 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testFloatUnion108 instanceof Float) {
                (encoder).writeIndex(1);
                (encoder).writeFloat(((Float) testFloatUnion108));
            }
        }
        (encoder).writeBoolean(((Boolean) data.get(10)));
        Boolean testBooleanUnion109 = ((Boolean) data.get(11));
        if (testBooleanUnion109 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testBooleanUnion109 instanceof Boolean) {
                (encoder).writeIndex(1);
                (encoder).writeBoolean(((Boolean) testBooleanUnion109));
            }
        }
        (encoder).writeBytes(((ByteBuffer) data.get(12)));
        ByteBuffer testBytesUnion110 = ((ByteBuffer) data.get(13));
        if (testBytesUnion110 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testBytesUnion110 instanceof ByteBuffer) {
                (encoder).writeIndex(1);
                (encoder).writeBytes(((ByteBuffer) testBytesUnion110));
            }
        }
    }

}
