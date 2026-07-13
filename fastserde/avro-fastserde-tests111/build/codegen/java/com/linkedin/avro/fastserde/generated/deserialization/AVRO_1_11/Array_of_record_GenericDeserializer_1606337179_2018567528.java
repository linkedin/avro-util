
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class Array_of_record_GenericDeserializer_1606337179_2018567528
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema unionOptionArrayElemSchema0;

    public Array_of_record_GenericDeserializer_1606337179_2018567528(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.unionOptionArrayElemSchema0 = readerSchema.getElementType();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"doc\":\"record\",\"fields\":[{\"name\":\"someInt\",\"type\":\"int\",\"doc\":\"\"}]}}");
        } else {
            if (unionIndex0 == 1) {
                List<IndexedRecord> unionOption0 = null;
                long chunkLen0 = (decoder.readArrayStart());
                if ((reuse) instanceof List) {
                    unionOption0 = ((List)(reuse));
                    if (unionOption0 instanceof GenericArray) {
                        ((GenericArray) unionOption0).reset();
                    } else {
                        unionOption0 .clear();
                    }
                } else {
                    unionOption0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), readerSchema);
                }
                while (chunkLen0 > 0) {
                    for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                        Object unionOptionArrayElementReuseVar0 = null;
                        if ((reuse) instanceof GenericArray) {
                            unionOptionArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                        }
                        unionOption0 .add(deserializerecord0(unionOptionArrayElementReuseVar0, (decoder), (customization)));
                    }
                    chunkLen0 = (decoder.arrayNext());
                }
                return unionOption0;
            } else {
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
            }
        }
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionArrayElemSchema0)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(unionOptionArrayElemSchema0);
        }
        record0 .put(0, (decoder.readInt()));
        return record0;
    }

}
