
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_7544686714080602018_7544686714080602018
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record899;
    private final Schema recordOptionSchema901;
    private final Schema subField903;
    private final Schema field905;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_7544686714080602018_7544686714080602018(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record899 = readerSchema.getField("record").schema();
        this.recordOptionSchema901 = record899 .getTypes().get(1);
        this.subField903 = recordOptionSchema901 .getField("subField").schema();
        this.field905 = readerSchema.getField("field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField898((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField898(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex900 = (decoder.readIndex());
        if (unionIndex900 == 0) {
            decoder.readNull();
        }
        if (unionIndex900 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(0, deserializesubRecord902(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(0), (decoder)));
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(1, deserializesubRecord902(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(1), (decoder)));
        int unionIndex906 = (decoder.readIndex());
        if (unionIndex906 == 0) {
            decoder.readNull();
        }
        if (unionIndex906 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(2) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(2))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(null));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
    }

    public IndexedRecord deserializesubRecord902(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordOptionSchema901)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordOptionSchema901);
        }
        int unionIndex904 = (decoder.readIndex());
        if (unionIndex904 == 0) {
            decoder.readNull();
        }
        if (unionIndex904 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
