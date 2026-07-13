
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly_GenericDeserializer_64996084_522115670
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema field10;
    private final Schema field20;
    private final Schema fieldRecordOne0;
    private final Schema recordOneRecordSchema0;
    private final Schema fieldA0;
    private final Schema arrayFieldRecordTwo0;
    private final Schema arrayFieldRecordTwoArraySchema0;
    private final Schema arrayFieldRecordTwoArrayElemSchema0;
    private final Schema recordTwoRecordSchema0;
    private final Schema fieldB0;

    public FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly_GenericDeserializer_64996084_522115670(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.field10 = readerSchema.getField("field1").schema();
        this.field20 = readerSchema.getField("field2").schema();
        this.fieldRecordOne0 = readerSchema.getField("fieldRecordOne").schema();
        this.recordOneRecordSchema0 = fieldRecordOne0 .getTypes().get(1);
        this.fieldA0 = recordOneRecordSchema0 .getField("fieldA").schema();
        this.arrayFieldRecordTwo0 = readerSchema.getField("arrayFieldRecordTwo").schema();
        this.arrayFieldRecordTwoArraySchema0 = arrayFieldRecordTwo0 .getTypes().get(1);
        this.arrayFieldRecordTwoArrayElemSchema0 = arrayFieldRecordTwoArraySchema0 .getElementType();
        this.recordTwoRecordSchema0 = arrayFieldRecordTwoArrayElemSchema0 .getTypes().get(1);
        this.fieldB0 = recordTwoRecordSchema0 .getField("fieldB").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Utf8 charSequence0;
        Object oldString0 = fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .put(0, charSequence0);
        populate_FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0((fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly1((fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .put(1, (decoder.readDouble()));
        fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .put(2, deserializerecordOne0(fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .get(2), (decoder), (customization)));
    }

    public IndexedRecord deserializerecordOne0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord recordOne0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordOneRecordSchema0)) {
            recordOne0 = ((IndexedRecord)(reuse));
        } else {
            recordOne0 = new org.apache.avro.generic.GenericData.Record(recordOneRecordSchema0);
        }
        Utf8 charSequence1;
        Object oldString1 = recordOne0 .get(0);
        if (oldString1 instanceof Utf8) {
            charSequence1 = (decoder).readString(((Utf8) oldString1));
        } else {
            charSequence1 = (decoder).readString(null);
        }
        recordOne0 .put(0, charSequence1);
        return recordOne0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly1(IndexedRecord fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> arrayFieldRecordTwo1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .get(3);
        if (oldArray0 instanceof List) {
            arrayFieldRecordTwo1 = ((List) oldArray0);
            if (arrayFieldRecordTwo1 instanceof GenericArray) {
                ((GenericArray) arrayFieldRecordTwo1).reset();
            } else {
                arrayFieldRecordTwo1 .clear();
            }
        } else {
            arrayFieldRecordTwo1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), arrayFieldRecordTwoArraySchema0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayFieldRecordTwoArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    arrayFieldRecordTwoArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                arrayFieldRecordTwo1 .add(deserializerecordTwo0(arrayFieldRecordTwoArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldHandleNullableCorrectly0 .put(3, arrayFieldRecordTwo1);
    }

    public IndexedRecord deserializerecordTwo0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord recordTwo0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordTwoRecordSchema0)) {
            recordTwo0 = ((IndexedRecord)(reuse));
        } else {
            recordTwo0 = new org.apache.avro.generic.GenericData.Record(recordTwoRecordSchema0);
        }
        Utf8 charSequence2;
        Object oldString2 = recordTwo0 .get(0);
        if (oldString2 instanceof Utf8) {
            charSequence2 = (decoder).readString(((Utf8) oldString2));
        } else {
            charSequence2 = (decoder).readString(null);
        }
        recordTwo0 .put(0, charSequence2);
        return recordTwo0;
    }

}
