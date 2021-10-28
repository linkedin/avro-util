
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields_GenericDeserializer_8274098733431623188_3676269449561141324
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record10;
    private final Schema subField0;
    private final Schema intFieldField0;
    private final Schema intField0;
    private final Schema subField1;
    private final Schema recordArray0;

    public FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields_GenericDeserializer_8274098733431623188_3676269449561141324(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record10 = readerSchema.getField("record1").schema();
        this.subField0 = record10 .getField("subField").schema();
        this.intFieldField0 = record10 .getField("intField").schema();
        this.intField0 = record10 .getField("intField").schema();
        this.subField1 = record10 .getField("subField").schema();
        this.recordArray0 = readerSchema.getField("recordArray").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.put(0, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.get(0), (decoder)));
        FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.put(1, deserializesubRecord1(FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.get(1), (decoder)));
        List<IndexedRecord> recordArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.get(2);
        if (oldArray0 instanceof List) {
            recordArray1 = ((List) oldArray0);
            recordArray1 .clear();
        } else {
            recordArray1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), recordArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object recordArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    recordArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                recordArray1 .add(deserializesubRecord0(recordArrayArrayElementReuseVar0, (decoder)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields.put(2, recordArray1);
        return FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == record10)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(record10);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                Object oldString0 = subRecord.get(1);
                if (oldString0 instanceof Utf8) {
                    subRecord.put(1, (decoder).readString(((Utf8) oldString0)));
                } else {
                    subRecord.put(1, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex0));
            }
        }
        subRecord.put(0, null);
        return subRecord;
    }

    public IndexedRecord deserializesubRecord1(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == record10)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(record10);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                subRecord.put(0, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'intField': "+ unionIndex1));
            }
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex2 == 1) {
                Object oldString1 = subRecord.get(1);
                if (oldString1 instanceof Utf8) {
                    subRecord.put(1, (decoder).readString(((Utf8) oldString1)));
                } else {
                    subRecord.put(1, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex2));
            }
        }
        return subRecord;
    }

}
