
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Array_of_record_GenericDeserializer_477614132_477614132
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema arrayArrayElemSchema0;
    private final Schema field0;

    public Array_of_record_GenericDeserializer_477614132_477614132(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema0 = readerSchema.getElementType();
        this.field0 = arrayArrayElemSchema0 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        List<IndexedRecord> array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof List) {
            array0 = ((List)(reuse));
            if (array0 instanceof GenericArray) {
                ((GenericArray) array0).reset();
            } else {
                array0 .clear();
            }
        } else {
            array0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), readerSchema);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayArrayElementReuseVar0 = null;
                if ((reuse) instanceof GenericArray) {
                    arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                }
                array0 .add(deserializerecord0(arrayArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayArrayElemSchema0)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(arrayArrayElemSchema0);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            record0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = record0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                record0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'field': "+ unionIndex0));
            }
        }
        return record0;
    }

}
