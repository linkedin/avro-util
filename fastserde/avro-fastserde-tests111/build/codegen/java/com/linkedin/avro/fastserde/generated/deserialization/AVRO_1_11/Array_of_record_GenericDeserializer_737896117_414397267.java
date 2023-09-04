
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class Array_of_record_GenericDeserializer_737896117_414397267
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema unionOptionArrayElemSchema0;

    public Array_of_record_GenericDeserializer_737896117_414397267(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.unionOptionArrayElemSchema0 = readerSchema.getElementType();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"com.adpilot.utils.generated.avro\",\"doc\":\"record\",\"fields\":[{\"name\":\"someInt\",\"type\":\"int\",\"doc\":\"\"}]}}");
        } else {
            if (unionIndex0 == 1) {
                List<IndexedRecord> unionOption0 = null;
                long chunkLen0 = (decoder.readArrayStart());
                if ((reuse) instanceof List) {
                    unionOption0 = ((List)(reuse));
                    unionOption0 .clear();
                } else {
                    unionOption0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), readerSchema);
                }
                while (chunkLen0 > 0) {
                    for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                        Object unionOptionArrayElementReuseVar0 = null;
                        if ((reuse) instanceof GenericArray) {
                            unionOptionArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                        }
                        unionOption0 .add(deserializerecord0(unionOptionArrayElementReuseVar0, (decoder)));
                    }
                    chunkLen0 = (decoder.arrayNext());
                }
                return unionOption0;
            } else {
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
            }
        }
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionArrayElemSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(unionOptionArrayElemSchema0);
        }
        record.put(0, (decoder.readInt()));
        return record;
    }

}
