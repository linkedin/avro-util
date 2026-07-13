
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_1966544736_1966544736
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

    public FastGenericDeserializerGeneratorTest_shouldReadPrimitives_GenericDeserializer_1966544736_1966544736(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testIntUnion0 = readerSchema.getField("testIntUnion").schema();
        this.testStringUnion0 = readerSchema.getField("testStringUnion").schema();
        this.testLongUnion0 = readerSchema.getField("testLongUnion").schema();
        this.testDoubleUnion0 = readerSchema.getField("testDoubleUnion").schema();
        this.testFloatUnion0 = readerSchema.getField("testFloatUnion").schema();
        this.testBooleanUnion0 = readerSchema.getField("testBooleanUnion").schema();
        this.testBytesUnion0 = readerSchema.getField("testBytesUnion").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPrimitives0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(0, (decoder.readInt()));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives0((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives1((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives2((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives3((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives4((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives5((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives6((fastGenericDeserializerGeneratorTest_shouldReadPrimitives0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadPrimitives0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
            }
        }
        Utf8 charSequence0;
        Object oldString0 = fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .get(2);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(2, charSequence0);
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives1(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(3, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence1;
                Object oldString1 = fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .get(3);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(3, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testStringUnion': "+ unionIndex1));
            }
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(4, (decoder.readLong()));
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives2(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(5, null);
        } else {
            if (unionIndex2 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(5, (decoder.readLong()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testLongUnion': "+ unionIndex2));
            }
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(6, (decoder.readDouble()));
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives3(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(7, null);
        } else {
            if (unionIndex3 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(7, (decoder.readDouble()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testDoubleUnion': "+ unionIndex3));
            }
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(8, (decoder.readFloat()));
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives4(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(9, null);
        } else {
            if (unionIndex4 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(9, (decoder.readFloat()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testFloatUnion': "+ unionIndex4));
            }
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(10, (decoder.readBoolean()));
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives5(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(11, null);
        } else {
            if (unionIndex5 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(11, (decoder.readBoolean()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testBooleanUnion': "+ unionIndex5));
            }
        }
        ByteBuffer byteBuffer0;
        Object oldBytes0 = fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .get(12);
        if (oldBytes0 instanceof ByteBuffer) {
            byteBuffer0 = (decoder).readBytes(((ByteBuffer) oldBytes0));
        } else {
            byteBuffer0 = (decoder).readBytes((null));
        }
        fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(12, byteBuffer0);
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPrimitives6(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadPrimitives0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(13, null);
        } else {
            if (unionIndex6 == 1) {
                ByteBuffer byteBuffer1;
                Object oldBytes1 = fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .get(13);
                if (oldBytes1 instanceof ByteBuffer) {
                    byteBuffer1 = (decoder).readBytes(((ByteBuffer) oldBytes1));
                } else {
                    byteBuffer1 = (decoder).readBytes((null));
                }
                fastGenericDeserializerGeneratorTest_shouldReadPrimitives0 .put(13, byteBuffer1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testBytesUnion': "+ unionIndex6));
            }
        }
    }

}
