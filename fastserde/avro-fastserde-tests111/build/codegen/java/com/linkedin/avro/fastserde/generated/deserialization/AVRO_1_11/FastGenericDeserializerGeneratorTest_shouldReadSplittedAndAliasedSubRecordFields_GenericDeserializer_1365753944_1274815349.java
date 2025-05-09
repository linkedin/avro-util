
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

public class FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields_GenericDeserializer_1365753944_1274815349
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record10;
    private final Schema subField0;
    private final Schema record20;
    private final Schema intField0;
    private final Schema subField1;
    private final Schema recordArray0;

    public FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields_GenericDeserializer_1365753944_1274815349(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record10 = readerSchema.getField("record1").schema();
        this.subField0 = record10 .getField("subField").schema();
        this.record20 = readerSchema.getField("record2").schema();
        this.intField0 = record20 .getField("intField").schema();
        this.subField1 = record20 .getField("subField").schema();
        this.recordArray0 = readerSchema.getField("recordArray").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .put(0, deserializealiasedSubRecord0(fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .get(0), (decoder), (customization)));
        populate_FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0((fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0;
    }

    public IndexedRecord deserializealiasedSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord aliasedSubRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == record10)) {
            aliasedSubRecord0 = ((IndexedRecord)(reuse));
        } else {
            aliasedSubRecord0 = new org.apache.avro.generic.GenericData.Record(record10);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                decoder.readInt();
            } else {
                throw new RuntimeException(("Illegal union index for 'intField': "+ unionIndex0));
            }
        }
        populate_aliasedSubRecord0((aliasedSubRecord0), (customization), (decoder));
        return aliasedSubRecord0;
    }

    private void populate_aliasedSubRecord0(IndexedRecord aliasedSubRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            aliasedSubRecord0 .put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence0;
                Object oldString0 = aliasedSubRecord0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                aliasedSubRecord0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex1));
            }
        }
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .put(1, deserializesubRecord0(fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .get(1), (decoder), (customization)));
        List<IndexedRecord> recordArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .get(2);
        if (oldArray0 instanceof List) {
            recordArray1 = ((List) oldArray0);
            if (recordArray1 instanceof GenericArray) {
                ((GenericArray) recordArray1).reset();
            } else {
                recordArray1 .clear();
            }
        } else {
            recordArray1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), recordArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object recordArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    recordArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                recordArray1 .add(deserializealiasedSubRecord0(recordArrayArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadSplittedAndAliasedSubRecordFields0 .put(2, recordArray1);
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord aliasedSubRecord1;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == record20)) {
            aliasedSubRecord1 = ((IndexedRecord)(reuse));
        } else {
            aliasedSubRecord1 = new org.apache.avro.generic.GenericData.Record(record20);
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            aliasedSubRecord1 .put(0, null);
        } else {
            if (unionIndex2 == 1) {
                aliasedSubRecord1 .put(0, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'intField': "+ unionIndex2));
            }
        }
        populate_aliasedSubRecord1((aliasedSubRecord1), (customization), (decoder));
        return aliasedSubRecord1;
    }

    private void populate_aliasedSubRecord1(IndexedRecord aliasedSubRecord1, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            aliasedSubRecord1 .put(1, null);
        } else {
            if (unionIndex3 == 1) {
                Utf8 charSequence1;
                Object oldString1 = aliasedSubRecord1 .get(1);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                aliasedSubRecord1 .put(1, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex3));
            }
        }
    }

}
