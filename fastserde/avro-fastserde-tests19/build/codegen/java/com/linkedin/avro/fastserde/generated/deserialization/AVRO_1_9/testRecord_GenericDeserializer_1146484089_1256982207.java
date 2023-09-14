
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class testRecord_GenericDeserializer_1146484089_1256982207
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;
    private final Map enumMappingtestEnum0;

    public testRecord_GenericDeserializer_1146484089_1256982207(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
        HashMap tempEnumMapping0 = new HashMap(3);
        tempEnumMapping0 .put(new Integer(0), new Integer(0));
        tempEnumMapping0 .put(new Integer(1), new Integer(1));
        tempEnumMapping0 .put(new Integer(2), new AvroTypeException("com.linkedin.avro.fastserde.generated.avro.testEnum: No match for C"));
        this.enumMappingtestEnum0 = Collections.unmodifiableMap(tempEnumMapping0);
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
        GenericEnumSymbol enumValue0 = null;
        Object enumIndexLookupResult0 = enumMappingtestEnum0 .get(enumIndex0);
        if (enumIndexLookupResult0 instanceof Integer) {
            enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(((Integer) enumIndexLookupResult0)));
        } else {
            if (enumIndexLookupResult0 instanceof AvroTypeException) {
                throw((AvroTypeException) enumIndexLookupResult0);
            } else {
                throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.testEnum': "+ enumIndex0));
            }
        }
        testRecord.put(0, enumValue0);
        return testRecord;
    }

}
