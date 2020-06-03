
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

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
    private final Schema arrayArrayElemSchema763;
    private final Schema field766;

    public Array_of_record_GenericDeserializer_1629046702287533603_1629046702287533603(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema763 = readerSchema.getElementType();
        this.field766 = arrayArrayElemSchema763 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> array759 = null;
        long chunkLen760 = (decoder.readArrayStart());
        if (chunkLen760 > 0) {
            List<IndexedRecord> arrayReuse761 = null;
            if ((reuse) instanceof List) {
                arrayReuse761 = ((List)(reuse));
            }
            if (arrayReuse761 != (null)) {
                arrayReuse761 .clear();
                array759 = arrayReuse761;
            } else {
                array759 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen760), readerSchema);
            }
            do {
                for (int counter762 = 0; (counter762 <chunkLen760); counter762 ++) {
                    Object arrayArrayElementReuseVar764 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar764 = ((GenericArray)(reuse)).peek();
                    }
                    array759 .add(deserializerecord765(arrayArrayElementReuseVar764, (decoder)));
                }
                chunkLen760 = (decoder.arrayNext());
            } while (chunkLen760 > 0);
        } else {
            array759 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, readerSchema);
        }
        return array759;
    }

    public IndexedRecord deserializerecord765(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayArrayElemSchema763)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(arrayArrayElemSchema763);
        }
        int unionIndex767 = (decoder.readIndex());
        if (unionIndex767 == 0) {
            decoder.readNull();
        }
        if (unionIndex767 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
