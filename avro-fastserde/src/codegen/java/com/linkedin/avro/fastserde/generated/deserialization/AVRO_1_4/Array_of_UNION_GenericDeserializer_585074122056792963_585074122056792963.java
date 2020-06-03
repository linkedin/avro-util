
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Array_of_UNION_GenericDeserializer_585074122056792963_585074122056792963
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema arrayArrayElemSchema772;
    private final Schema arrayElemOptionSchema775;
    private final Schema field777;

    public Array_of_UNION_GenericDeserializer_585074122056792963_585074122056792963(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema772 = readerSchema.getElementType();
        this.arrayElemOptionSchema775 = arrayArrayElemSchema772 .getTypes().get(1);
        this.field777 = arrayElemOptionSchema775 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> array768 = null;
        long chunkLen769 = (decoder.readArrayStart());
        if (chunkLen769 > 0) {
            List<IndexedRecord> arrayReuse770 = null;
            if ((reuse) instanceof List) {
                arrayReuse770 = ((List)(reuse));
            }
            if (arrayReuse770 != (null)) {
                arrayReuse770 .clear();
                array768 = arrayReuse770;
            } else {
                array768 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen769), readerSchema);
            }
            do {
                for (int counter771 = 0; (counter771 <chunkLen769); counter771 ++) {
                    Object arrayArrayElementReuseVar773 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar773 = ((GenericArray)(reuse)).peek();
                    }
                    int unionIndex774 = (decoder.readIndex());
                    if (unionIndex774 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex774 == 1) {
                        array768 .add(deserializerecord776(arrayArrayElementReuseVar773, (decoder)));
                    }
                }
                chunkLen769 = (decoder.arrayNext());
            } while (chunkLen769 > 0);
        } else {
            array768 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, readerSchema);
        }
        return array768;
    }

    public IndexedRecord deserializerecord776(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayElemOptionSchema775)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(arrayElemOptionSchema775);
        }
        int unionIndex778 = (decoder.readIndex());
        if (unionIndex778 == 0) {
            decoder.readNull();
        }
        if (unionIndex778 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
