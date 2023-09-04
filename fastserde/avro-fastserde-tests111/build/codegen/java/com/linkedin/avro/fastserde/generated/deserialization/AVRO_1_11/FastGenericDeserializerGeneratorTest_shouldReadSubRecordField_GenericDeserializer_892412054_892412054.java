
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_892412054_892412054
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record0;
    private final Schema recordOptionSchema0;
    private final Schema subField0;
    private final Schema field0;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_892412054_892412054(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record0 = readerSchema.getField("record").schema();
        this.recordOptionSchema0 = record0 .getTypes().get(1);
        this.subField0 = recordOptionSchema0 .getField("subField").schema();
        this.field0 = readerSchema.getField("field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(0, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(0), (decoder)));
            } else {
                throw new RuntimeException(("Illegal union index for 'record': "+ unionIndex0));
            }
        }
        populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordField0((FastGenericDeserializerGeneratorTest_shouldReadSubRecordField), (decoder));
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordField;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordOptionSchema0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            subRecord.put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Object oldString0 = subRecord.get(0);
                if (oldString0 instanceof Utf8) {
                    subRecord.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    subRecord.put(0, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex1));
            }
        }
        return subRecord;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordField0(IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordField, Decoder decoder)
        throws IOException
    {
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(1, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(1), (decoder)));
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, null);
        } else {
            if (unionIndex2 == 1) {
                Object oldString1 = FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.get(2);
                if (oldString1 instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(((Utf8) oldString1)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadSubRecordField.put(2, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'field': "+ unionIndex2));
            }
        }
    }

}
