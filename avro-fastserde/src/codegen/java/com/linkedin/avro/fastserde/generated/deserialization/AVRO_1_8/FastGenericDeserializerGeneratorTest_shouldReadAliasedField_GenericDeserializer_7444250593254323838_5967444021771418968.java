
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadAliasedField_GenericDeserializer_7444250593254323838_5967444021771418968
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testString588;
    private final Schema testStringUnionAlias590;

    public FastGenericDeserializerGeneratorTest_shouldReadAliasedField_GenericDeserializer_7444250593254323838_5967444021771418968(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testString588 = readerSchema.getField("testString").schema();
        this.testStringUnionAlias590 = readerSchema.getField("testStringUnionAlias").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField587((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField587(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadAliasedField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadAliasedField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadAliasedField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex589 = (decoder.readIndex());
        if (unionIndex589 == 0) {
            decoder.readNull();
        }
        if (unionIndex589 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(0) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(0))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(0, (decoder).readString(null));
            }
        }
        int unionIndex591 = (decoder.readIndex());
        if (unionIndex591 == 0) {
            decoder.readNull();
        }
        if (unionIndex591 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(1) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(1, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(1))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(1, (decoder).readString(null));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadAliasedField;
    }

}
