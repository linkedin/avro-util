
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes_GenericDeserializer_1116624465_531103201
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;
    private final Schema testEnumEnumSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes_GenericDeserializer_1116624465_531103201(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
        this.testEnumEnumSchema0 = testEnum0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        fastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0 .put(0, new org.apache.avro.generic.GenericData.EnumSymbol(testEnumEnumSchema0, testEnumEnumSchema0 .getEnumSymbols().get((decoder.readEnum()))));
        return fastGenericDeserializerGeneratorTest_shouldReadNonUnionEnumTypesWithUnionEnumTypes0;
    }

}
