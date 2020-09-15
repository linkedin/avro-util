
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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
    private final Schema testString0;
    private final Schema testStringUnionAlias0;

    public FastGenericDeserializerGeneratorTest_shouldReadAliasedField_GenericDeserializer_7444250593254323838_5967444021771418968(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testString0 = readerSchema.getField("testString").schema();
        this.testStringUnionAlias0 = readerSchema.getField("testStringUnionAlias").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadAliasedField0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadAliasedField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadAliasedField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadAliasedField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString0 = FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(0);
                if (oldString0 instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(0, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testString': "+ unionIndex0));
        }
        int unionIndex1 = (decoder.readIndex());
        switch (unionIndex1) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString1 = FastGenericDeserializerGeneratorTest_shouldReadAliasedField.get(1);
                if (oldString1 instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(1, (decoder).readString(((Utf8) oldString1)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadAliasedField.put(1, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testStringUnionAlias': "+ unionIndex1));
        }
        return FastGenericDeserializerGeneratorTest_shouldReadAliasedField;
    }

}
