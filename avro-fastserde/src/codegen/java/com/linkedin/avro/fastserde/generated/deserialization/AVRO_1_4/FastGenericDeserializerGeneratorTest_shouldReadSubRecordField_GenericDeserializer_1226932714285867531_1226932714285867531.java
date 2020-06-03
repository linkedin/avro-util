
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_1226932714285867531_1226932714285867531
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record1045;
    private final Schema recordOptionSchema1047;
    private final Schema subField1049;
    private final Schema field1051;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_1226932714285867531_1226932714285867531(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record1045 = readerSchema.getField("record").schema();
        this.recordOptionSchema1047 = record1045 .getTypes().get(1);
        this.subField1049 = recordOptionSchema1047 .getField("subField").schema();
        this.field1051 = readerSchema.getField("field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField1044((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField1044(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex1046 = (decoder.readIndex());
        if (unionIndex1046 == 0) {
            decoder.readNull();
        }
        if (unionIndex1046 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(0, deserializesubRecord1048(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(0), (decoder)));
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(1, deserializesubRecord1048(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(1), (decoder)));
        int unionIndex1052 = (decoder.readIndex());
        if (unionIndex1052 == 0) {
            decoder.readNull();
        }
        if (unionIndex1052 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(2) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(2))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(null));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
    }

    public IndexedRecord deserializesubRecord1048(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordOptionSchema1047)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordOptionSchema1047);
        }
        int unionIndex1050 = (decoder.readIndex());
        if (unionIndex1050 == 0) {
            decoder.readNull();
        }
        if (unionIndex1050 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
