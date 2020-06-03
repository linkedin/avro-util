
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Array_of_record_GenericDeserializer_1629046702287533603_1629046702287533603
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema arrayArrayElemSchema617;
    private final Schema field620;

    public Array_of_record_GenericDeserializer_1629046702287533603_1629046702287533603(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema617 = readerSchema.getElementType();
        this.field620 = arrayArrayElemSchema617 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> array613 = null;
        long chunkLen614 = (decoder.readArrayStart());
        if (chunkLen614 > 0) {
            List<IndexedRecord> arrayReuse615 = null;
            if ((reuse) instanceof List) {
                arrayReuse615 = ((List)(reuse));
            }
            if (arrayReuse615 != (null)) {
                arrayReuse615 .clear();
                array613 = arrayReuse615;
            } else {
                array613 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen614), readerSchema);
            }
            do {
                for (int counter616 = 0; (counter616 <chunkLen614); counter616 ++) {
                    Object arrayArrayElementReuseVar618 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar618 = ((GenericArray)(reuse)).peek();
                    }
                    array613 .add(deserializerecord619(arrayArrayElementReuseVar618, (decoder)));
                }
                chunkLen614 = (decoder.arrayNext());
            } while (chunkLen614 > 0);
        } else {
            array613 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, readerSchema);
        }
        return array613;
    }

    public IndexedRecord deserializerecord619(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayArrayElemSchema617)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(arrayArrayElemSchema617);
        }
        int unionIndex621 = (decoder.readIndex());
        if (unionIndex621 == 0) {
            decoder.readNull();
        }
        if (unionIndex621 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
