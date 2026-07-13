
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class message_GenericDeserializer_1140400810_2009870439
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema data0;
    private final Schema dataArraySchema0;
    private final Schema dataArrayElemSchema0;

    public message_GenericDeserializer_1140400810_2009870439(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.data0 = readerSchema.getField("data").schema();
        this.dataArraySchema0 = data0 .getTypes().get(1);
        this.dataArrayElemSchema0 = dataArraySchema0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializemessage0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializemessage0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord message0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            message0 = ((IndexedRecord)(reuse));
        } else {
            message0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<List<Integer>> data1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = message0 .get(0);
        if (oldArray0 instanceof List) {
            data1 = ((List) oldArray0);
            if (data1 instanceof GenericArray) {
                ((GenericArray) data1).reset();
            } else {
                data1 .clear();
            }
        } else {
            data1 = new org.apache.avro.generic.GenericData.Array<List<Integer>>(((int) chunkLen0), dataArraySchema0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object dataArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    dataArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                PrimitiveIntList dataElem0 = null;
                long chunkLen1 = (decoder.readArrayStart());
                if (dataArrayElementReuseVar0 instanceof PrimitiveIntList) {
                    dataElem0 = ((PrimitiveIntList) dataArrayElementReuseVar0);
                    dataElem0 .clear();
                } else {
                    dataElem0 = new PrimitiveIntArrayList(((int) chunkLen1));
                }
                while (chunkLen1 > 0) {
                    for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                        dataElem0 .addPrimitive((decoder.readInt()));
                    }
                    chunkLen1 = (decoder.arrayNext());
                }
                data1 .add(dataElem0);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        message0 .put(0, data1);
        return message0;
    }

}
