
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

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
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(1, (decoder.readInt()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
        }
        Object oldString0 = FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(2);
        if (oldString0 instanceof Utf8) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(((Utf8) oldString0)));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(2, (decoder).readString(null));
        }
        int unionIndex1 = (decoder.readIndex());
        switch (unionIndex1) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString1 = FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(3);
                if (oldString1 instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(((Utf8) oldString1)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(3, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testStringUnion': "+ unionIndex1));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(4, (decoder.readLong()));
        int unionIndex2 = (decoder.readIndex());
        switch (unionIndex2) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(5, (decoder.readLong()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testLongUnion': "+ unionIndex2));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(6, (decoder.readDouble()));
        int unionIndex3 = (decoder.readIndex());
        switch (unionIndex3) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(7, (decoder.readDouble()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testDoubleUnion': "+ unionIndex3));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(8, (decoder.readFloat()));
        int unionIndex4 = (decoder.readIndex());
        switch (unionIndex4) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(9, (decoder.readFloat()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testFloatUnion': "+ unionIndex4));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(10, (decoder.readBoolean()));
        int unionIndex5 = (decoder.readIndex());
        switch (unionIndex5) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(11, (decoder.readBoolean()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testBooleanUnion': "+ unionIndex5));
        }
        Object oldBytes0 = FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(12);
        if (oldBytes0 instanceof ByteBuffer) {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes(((ByteBuffer) oldBytes0)));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(12, (decoder).readBytes((null)));
        }
        int unionIndex6 = (decoder.readIndex());
        switch (unionIndex6) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldBytes1 = FastGenericDeserializerGeneratorTest_shouldReadPrimitives.get(13);
                if (oldBytes1 instanceof ByteBuffer) {
                    FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes(((ByteBuffer) oldBytes1)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadPrimitives.put(13, (decoder).readBytes((null)));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testBytesUnion': "+ unionIndex6));
        }
        return FastGenericDeserializerGeneratorTest_shouldReadPrimitives;
    }

}
