
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_6705572650240186052_6705572650240186052
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testIntUnion781;
    private final Schema testStringUnion783;
    private final Schema testLongUnion785;
    private final Schema testDoubleUnion787;
    private final Schema testFloatUnion789;
    private final Schema testBooleanUnion791;
    private final Schema testBytesUnion793;

    public FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_6705572650240186052_6705572650240186052(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testIntUnion781 = readerSchema.getField("testIntUnion").schema();
        this.testStringUnion783 = readerSchema.getField("testStringUnion").schema();
        this.testLongUnion785 = readerSchema.getField("testLongUnion").schema();
        this.testDoubleUnion787 = readerSchema.getField("testDoubleUnion").schema();
        this.testFloatUnion789 = readerSchema.getField("testFloatUnion").schema();
        this.testBooleanUnion791 = readerSchema.getField("testBooleanUnion").schema();
        this.testBytesUnion793 = readerSchema.getField("testBytesUnion").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives780((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives780(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(0, (decoder.readInt()));
        int unionIndex782 = (decoder.readIndex());
        if (unionIndex782 == 0) {
            decoder.readNull();
        }
        if (unionIndex782 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(1, (decoder.readInt()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2) instanceof Utf8) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(null));
        }
        int unionIndex784 = (decoder.readIndex());
        if (unionIndex784 == 0) {
            decoder.readNull();
        }
        if (unionIndex784 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(null));
            }
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(4, (decoder.readLong()));
        int unionIndex786 = (decoder.readIndex());
        if (unionIndex786 == 0) {
            decoder.readNull();
        }
        if (unionIndex786 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(5, (decoder.readLong()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(6, (decoder.readDouble()));
        int unionIndex788 = (decoder.readIndex());
        if (unionIndex788 == 0) {
            decoder.readNull();
        }
        if (unionIndex788 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(7, (decoder.readDouble()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(8, (decoder.readFloat()));
        int unionIndex790 = (decoder.readIndex());
        if (unionIndex790 == 0) {
            decoder.readNull();
        }
        if (unionIndex790 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(9, (decoder.readFloat()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(10, (decoder.readBoolean()));
        int unionIndex792 = (decoder.readIndex());
        if (unionIndex792 == 0) {
            decoder.readNull();
        }
        if (unionIndex792 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(11, (decoder.readBoolean()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12) instanceof ByteBuffer) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes((null)));
        }
        int unionIndex794 = (decoder.readIndex());
        if (unionIndex794 == 0) {
            decoder.readNull();
        }
        if (unionIndex794 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13) instanceof ByteBuffer) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes((null)));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
    }

}
