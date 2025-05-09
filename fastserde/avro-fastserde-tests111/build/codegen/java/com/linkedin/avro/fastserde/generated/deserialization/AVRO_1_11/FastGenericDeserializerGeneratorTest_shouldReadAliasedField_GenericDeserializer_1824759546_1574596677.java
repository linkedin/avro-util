
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadAliasedField_GenericDeserializer_1824759546_1574596677
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testString0;
    private final Schema testStringUnionAlias0;

    public FastGenericDeserializerGeneratorTest_shouldReadAliasedField_GenericDeserializer_1824759546_1574596677(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testString0 = readerSchema.getField("testString").schema();
        this.testStringUnionAlias0 = readerSchema.getField("testStringUnionAlias").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadAliasedField0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'testString': "+ unionIndex0));
            }
        }
        populate_FastGenericDeserializerGeneratorTest_shouldReadAliasedField0((fastGenericDeserializerGeneratorTest_shouldReadAliasedField0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadAliasedField0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadAliasedField0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadAliasedField0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .put(1, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence1;
                Object oldString1 = fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .get(1);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                fastGenericDeserializerGeneratorTest_shouldReadAliasedField0 .put(1, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testStringUnionAlias': "+ unionIndex1));
            }
        }
    }

}
