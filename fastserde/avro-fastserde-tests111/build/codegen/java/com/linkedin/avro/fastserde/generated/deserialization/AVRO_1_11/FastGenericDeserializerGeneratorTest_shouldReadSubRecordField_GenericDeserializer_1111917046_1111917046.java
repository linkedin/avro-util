
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_1111917046_1111917046
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema record0;
    private final Schema recordOptionSchema0;
    private final Schema subField0;
    private final Schema field0;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordField_GenericDeserializer_1111917046_1111917046(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.record0 = readerSchema.getField("record").schema();
        this.recordOptionSchema0 = record0 .getTypes().get(1);
        this.subField0 = recordOptionSchema0 .getField("subField").schema();
        this.field0 = readerSchema.getField("field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordField0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .put(0, deserializesubRecord0(fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .get(0), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'record': "+ unionIndex0));
            }
        }
        populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordField0((fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordOptionSchema0)) {
            subRecord0 = ((IndexedRecord)(reuse));
        } else {
            subRecord0 = new org.apache.avro.generic.GenericData.Record(recordOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            subRecord0 .put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence0;
                Object oldString0 = subRecord0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                subRecord0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex1));
            }
        }
        return subRecord0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordField0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .put(1, deserializesubRecord0(fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .get(1), (decoder), (customization)));
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .put(2, null);
        } else {
            if (unionIndex2 == 1) {
                Utf8 charSequence1;
                Object oldString1 = fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .get(2);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                fastGenericDeserializerGeneratorTest_shouldReadSubRecordField0 .put(2, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'field': "+ unionIndex2));
            }
        }
    }

}
