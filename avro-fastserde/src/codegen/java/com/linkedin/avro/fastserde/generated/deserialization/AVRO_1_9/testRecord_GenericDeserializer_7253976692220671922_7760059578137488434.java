
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class testRecord_GenericDeserializer_7253976692220671922_7760059578137488434
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;

    public testRecord_GenericDeserializer_7253976692220671922_7760059578137488434(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializetestRecord0((reuse), (decoder));
    }

    public IndexedRecord deserializetestRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord testRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            testRecord = ((IndexedRecord)(reuse));
        } else {
            testRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int enumIndex0 = (decoder.readEnum());
        org.apache.avro.generic.GenericData.EnumSymbol enumValue0 = null;
        switch (enumIndex0) {
            case  0 :
                enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(0));
                break;
            case  1 :
                enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(1));
                break;
            case  2 :
                throw new AvroTypeException("com.adpilot.utils.generated.avro.testEnum: No match for C");
            default:
                throw new RuntimeException(("Illegal enum index for 'com.adpilot.utils.generated.avro.testEnum': "+ enumIndex0));
        }
        testRecord.put(0, enumValue0);
        return testRecord;
    }

}
