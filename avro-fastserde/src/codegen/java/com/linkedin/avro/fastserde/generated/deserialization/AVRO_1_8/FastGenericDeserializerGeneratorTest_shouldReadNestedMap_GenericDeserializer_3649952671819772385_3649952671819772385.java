
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_3649952671819772385_3649952671819772385
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema mapField0;
    private final Schema mapFieldMapValueSchema0;
    private final Schema mapFieldValueMapValueSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_3649952671819772385_3649952671819772385(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapField0 = readerSchema.getField("mapField").schema();
        this.mapFieldMapValueSchema0 = mapField0 .getValueType();
        this.mapFieldValueMapValueSchema0 = mapFieldMapValueSchema0 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, Map<Utf8, List<Integer>>> mapField1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            Map<Utf8, Map<Utf8, List<Integer>>> mapFieldReuse0 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0) instanceof Map) {
                mapFieldReuse0 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0));
            }
            if (mapFieldReuse0 != (null)) {
                mapFieldReuse0 .clear();
                mapField1 = mapFieldReuse0;
            } else {
                mapField1 = new HashMap<Utf8, Map<Utf8, List<Integer>>>();
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    Map<Utf8, List<Integer>> mapFieldValue0 = null;
                    long chunkLen1 = (decoder.readMapStart());
                    if (chunkLen1 > 0) {
                        Map<Utf8, List<Integer>> mapFieldValueReuse0 = null;
                        if (null instanceof Map) {
                            mapFieldValueReuse0 = ((Map) null);
                        }
                        if (mapFieldValueReuse0 != (null)) {
                            mapFieldValueReuse0 .clear();
                            mapFieldValue0 = mapFieldValueReuse0;
                        } else {
                            mapFieldValue0 = new HashMap<Utf8, List<Integer>>();
                        }
                        do {
                            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                                Utf8 key1 = (decoder.readString(null));
                                PrimitiveIntList mapFieldValueValue0 = null;
                                long chunkLen2 = (decoder.readArrayStart());
                                if (null instanceof PrimitiveIntList) {
                                    mapFieldValueValue0 = ((PrimitiveIntList) null);
                                    mapFieldValueValue0 .clear();
                                } else {
                                    mapFieldValueValue0 = new PrimitiveIntArrayList(((int) chunkLen2));
                                }
                                while (chunkLen2 > 0) {
                                    for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                                        mapFieldValueValue0 .addPrimitive((decoder.readInt()));
                                    }
                                    chunkLen2 = (decoder.arrayNext());
                                }
                                mapFieldValue0 .put(key1, mapFieldValueValue0);
                            }
                            chunkLen1 = (decoder.mapNext());
                        } while (chunkLen1 > 0);
                    } else {
                        mapFieldValue0 = Collections.emptyMap();
                    }
                    mapField1 .put(key0, mapFieldValue0);
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            mapField1 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadNestedMap.put(0, mapField1);
        return FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
    }

}
