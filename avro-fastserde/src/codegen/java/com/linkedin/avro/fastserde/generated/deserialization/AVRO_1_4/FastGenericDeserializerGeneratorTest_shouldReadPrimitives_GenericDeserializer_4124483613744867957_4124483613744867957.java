
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_4124483613744867957_4124483613744867957
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testIntUnion0;
    private final Schema testStringUnion0;
    private final Schema testLongUnion0;
    private final Schema testDoubleUnion0;
    private final Schema testFloatUnion0;
    private final Schema testBooleanUnion0;
    private final Schema testBytesUnion0;

    public FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_4124483613744867957_4124483613744867957(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testIntUnion0 = readerSchema.getField("testIntUnion").schema();
        this.testStringUnion0 = readerSchema.getField("testStringUnion").schema();
        this.testLongUnion0 = readerSchema.getField("testLongUnion").schema();
        this.testDoubleUnion0 = readerSchema.getField("testDoubleUnion").schema();
        this.testFloatUnion0 = readerSchema.getField("testFloatUnion").schema();
        this.testBooleanUnion0 = readerSchema.getField("testBooleanUnion").schema();
        this.testBytesUnion0 = readerSchema.getField("testBytesUnion").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(0, (decoder.readInt()));
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        }
        if (unionIndex0 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(1, (decoder.readInt()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2) instanceof Utf8) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(null));
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        }
        if (unionIndex1 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(null));
            }
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(4, (decoder.readLong()));
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
        }
        if (unionIndex2 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(5, (decoder.readLong()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(6, (decoder.readDouble()));
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
        }
        if (unionIndex3 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(7, (decoder.readDouble()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(8, (decoder.readFloat()));
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
        }
        if (unionIndex4 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(9, (decoder.readFloat()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(10, (decoder.readBoolean()));
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
        }
        if (unionIndex5 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(11, (decoder.readBoolean()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12) instanceof ByteBuffer) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes((null)));
        }
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            decoder.readNull();
        }
        if (unionIndex6 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13) instanceof ByteBuffer) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes((null)));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
    }

}
