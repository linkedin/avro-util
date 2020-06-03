
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
    private final Schema testIntUnion927;
    private final Schema testStringUnion929;
    private final Schema testLongUnion931;
    private final Schema testDoubleUnion933;
    private final Schema testFloatUnion935;
    private final Schema testBooleanUnion937;
    private final Schema testBytesUnion939;

    public FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_4124483613744867957_4124483613744867957(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testIntUnion927 = readerSchema.getField("testIntUnion").schema();
        this.testStringUnion929 = readerSchema.getField("testStringUnion").schema();
        this.testLongUnion931 = readerSchema.getField("testLongUnion").schema();
        this.testDoubleUnion933 = readerSchema.getField("testDoubleUnion").schema();
        this.testFloatUnion935 = readerSchema.getField("testFloatUnion").schema();
        this.testBooleanUnion937 = readerSchema.getField("testBooleanUnion").schema();
        this.testBytesUnion939 = readerSchema.getField("testBytesUnion").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives926((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives926(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(0, (decoder.readInt()));
        int unionIndex928 = (decoder.readIndex());
        if (unionIndex928 == 0) {
            decoder.readNull();
        }
        if (unionIndex928 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(1, (decoder.readInt()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2) instanceof Utf8) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(null));
        }
        int unionIndex930 = (decoder.readIndex());
        if (unionIndex930 == 0) {
            decoder.readNull();
        }
        if (unionIndex930 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(null));
            }
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(4, (decoder.readLong()));
        int unionIndex932 = (decoder.readIndex());
        if (unionIndex932 == 0) {
            decoder.readNull();
        }
        if (unionIndex932 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(5, (decoder.readLong()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(6, (decoder.readDouble()));
        int unionIndex934 = (decoder.readIndex());
        if (unionIndex934 == 0) {
            decoder.readNull();
        }
        if (unionIndex934 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(7, (decoder.readDouble()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(8, (decoder.readFloat()));
        int unionIndex936 = (decoder.readIndex());
        if (unionIndex936 == 0) {
            decoder.readNull();
        }
        if (unionIndex936 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(9, (decoder.readFloat()));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(10, (decoder.readBoolean()));
        int unionIndex938 = (decoder.readIndex());
        if (unionIndex938 == 0) {
            decoder.readNull();
        }
        if (unionIndex938 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(11, (decoder.readBoolean()));
        }
        if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12) instanceof ByteBuffer) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12))));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes((null)));
        }
        int unionIndex940 = (decoder.readIndex());
        if (unionIndex940 == 0) {
            decoder.readNull();
        }
        if (unionIndex940 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13) instanceof ByteBuffer) {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes(((ByteBuffer) FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes((null)));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
    }

}
