
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_1191367445_653449771
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;

    public record_GenericDeserializer_1191367445_653449771(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"array\",\"items\":\"int\"}");
        } else {
            if (unionIndex0 == 1) {
                PrimitiveIntList someIntsOption0 = null;
                long chunkLen0 = (decoder.readArrayStart());
                Object oldArray0 = record0 .get(0);
                if (oldArray0 instanceof PrimitiveIntList) {
                    someIntsOption0 = ((PrimitiveIntList) oldArray0);
                    someIntsOption0 .clear();
                } else {
                    someIntsOption0 = new PrimitiveIntArrayList(((int) chunkLen0));
                }
                while (chunkLen0 > 0) {
                    for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                        someIntsOption0 .addPrimitive((decoder.readInt()));
                    }
                    chunkLen0 = (decoder.arrayNext());
                }
                record0 .put(0, someIntsOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'someInts': "+ unionIndex0));
            }
        }
        return record0;
    }

}
