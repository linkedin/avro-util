
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastStringableTest_javaStringPropertyTest_GenericDeserializer_9185875457190242821_9185875457190242821
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;

    public FastStringableTest_javaStringPropertyTest_GenericDeserializer_9185875457190242821_9185875457190242821(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastStringableTest_javaStringPropertyTest0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastStringableTest_javaStringPropertyTest0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastStringableTest_javaStringPropertyTest;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastStringableTest_javaStringPropertyTest = ((IndexedRecord)(reuse));
        } else {
            FastStringableTest_javaStringPropertyTest = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        if (FastStringableTest_javaStringPropertyTest.get(0) instanceof Utf8) {
            FastStringableTest_javaStringPropertyTest.put(0, (decoder).readString(((Utf8) FastStringableTest_javaStringPropertyTest.get(0))));
        } else {
            FastStringableTest_javaStringPropertyTest.put(0, (decoder).readString(null));
        }
        return FastStringableTest_javaStringPropertyTest;
    }

}
