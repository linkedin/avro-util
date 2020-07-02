
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastStringableTest_javaStringPropertyInsideUnionTest_GenericDeserializer_4476580127917956418_4476580127917956418
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema favorite_number0;
    private final Schema favorite_color0;

    public FastStringableTest_javaStringPropertyInsideUnionTest_GenericDeserializer_4476580127917956418_4476580127917956418(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.favorite_number0 = readerSchema.getField("favorite_number").schema();
        this.favorite_color0 = readerSchema.getField("favorite_color").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastStringableTest_javaStringPropertyInsideUnionTest0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastStringableTest_javaStringPropertyInsideUnionTest0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastStringableTest_javaStringPropertyInsideUnionTest;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastStringableTest_javaStringPropertyInsideUnionTest = ((IndexedRecord)(reuse));
        } else {
            FastStringableTest_javaStringPropertyInsideUnionTest = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastStringableTest_javaStringPropertyInsideUnionTest.put(0, (decoder).readString());
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                FastStringableTest_javaStringPropertyInsideUnionTest.put(1, (decoder.readInt()));
            }
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                FastStringableTest_javaStringPropertyInsideUnionTest.put(2, (decoder).readString());
            }
        }
        return FastStringableTest_javaStringPropertyInsideUnionTest;
    }

}
