
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_8750596110605641040_8750596110605641040
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema mapField0;
    private final Schema mapFieldMapValueSchema0;
    private final Schema mapFieldValueMapValueSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_8750596110605641040_8750596110605641040(Schema readerSchema) {
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
                                List<Integer> mapFieldValueValue0 = null;
                                long chunkLen2 = (decoder.readArrayStart());
                                if (chunkLen2 > 0) {
                                    List<Integer> mapFieldValueValueReuse0 = null;
                                    if (null instanceof List) {
                                        mapFieldValueValueReuse0 = ((List) null);
                                    }
                                    if (mapFieldValueValueReuse0 != (null)) {
                                        mapFieldValueValueReuse0 .clear();
                                        mapFieldValueValue0 = mapFieldValueValueReuse0;
                                    } else {
                                        mapFieldValueValue0 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen2), mapFieldValueMapValueSchema0);
                                    }
                                    do {
                                        for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                                            Object mapFieldValueValueArrayElementReuseVar0 = null;
                                            if (null instanceof GenericArray) {
                                                mapFieldValueValueArrayElementReuseVar0 = ((GenericArray) null).peek();
                                            }
                                            mapFieldValueValue0 .add((decoder.readInt()));
                                        }
                                        chunkLen2 = (decoder.arrayNext());
                                    } while (chunkLen2 > 0);
                                } else {
                                    mapFieldValueValue0 = new org.apache.avro.generic.GenericData.Array<Integer>(0, mapFieldValueMapValueSchema0);
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
