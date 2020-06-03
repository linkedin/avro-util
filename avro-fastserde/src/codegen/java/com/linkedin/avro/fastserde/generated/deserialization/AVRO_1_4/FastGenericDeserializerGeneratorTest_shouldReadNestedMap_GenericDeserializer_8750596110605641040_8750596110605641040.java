
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
    private final Schema mapField882;
    private final Schema mapFieldMapValueSchema888;
    private final Schema mapFieldValueMapValueSchema894;

    public FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_8750596110605641040_8750596110605641040(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapField882 = readerSchema.getField("mapField").schema();
        this.mapFieldMapValueSchema888 = mapField882 .getValueType();
        this.mapFieldValueMapValueSchema894 = mapFieldMapValueSchema888 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap881((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap881(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, Map<Utf8, List<Integer>>> mapField883 = null;
        long chunkLen884 = (decoder.readMapStart());
        if (chunkLen884 > 0) {
            Map<Utf8, Map<Utf8, List<Integer>>> mapFieldReuse885 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0) instanceof Map) {
                mapFieldReuse885 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0));
            }
            if (mapFieldReuse885 != (null)) {
                mapFieldReuse885 .clear();
                mapField883 = mapFieldReuse885;
            } else {
                mapField883 = new HashMap<Utf8, Map<Utf8, List<Integer>>>();
            }
            do {
                for (int counter886 = 0; (counter886 <chunkLen884); counter886 ++) {
                    Utf8 key887 = (decoder.readString(null));
                    Map<Utf8, List<Integer>> mapFieldValue889 = null;
                    long chunkLen890 = (decoder.readMapStart());
                    if (chunkLen890 > 0) {
                        Map<Utf8, List<Integer>> mapFieldValueReuse891 = null;
                        if (null instanceof Map) {
                            mapFieldValueReuse891 = ((Map) null);
                        }
                        if (mapFieldValueReuse891 != (null)) {
                            mapFieldValueReuse891 .clear();
                            mapFieldValue889 = mapFieldValueReuse891;
                        } else {
                            mapFieldValue889 = new HashMap<Utf8, List<Integer>>();
                        }
                        do {
                            for (int counter892 = 0; (counter892 <chunkLen890); counter892 ++) {
                                Utf8 key893 = (decoder.readString(null));
                                List<Integer> mapFieldValueValue895 = null;
                                long chunkLen896 = (decoder.readArrayStart());
                                if (chunkLen896 > 0) {
                                    List<Integer> mapFieldValueValueReuse897 = null;
                                    if (null instanceof List) {
                                        mapFieldValueValueReuse897 = ((List) null);
                                    }
                                    if (mapFieldValueValueReuse897 != (null)) {
                                        mapFieldValueValueReuse897 .clear();
                                        mapFieldValueValue895 = mapFieldValueValueReuse897;
                                    } else {
                                        mapFieldValueValue895 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen896), mapFieldValueMapValueSchema894);
                                    }
                                    do {
                                        for (int counter898 = 0; (counter898 <chunkLen896); counter898 ++) {
                                            Object mapFieldValueValueArrayElementReuseVar899 = null;
                                            if (null instanceof GenericArray) {
                                                mapFieldValueValueArrayElementReuseVar899 = ((GenericArray) null).peek();
                                            }
                                            mapFieldValueValue895 .add((decoder.readInt()));
                                        }
                                        chunkLen896 = (decoder.arrayNext());
                                    } while (chunkLen896 > 0);
                                } else {
                                    mapFieldValueValue895 = new org.apache.avro.generic.GenericData.Array<Integer>(0, mapFieldValueMapValueSchema894);
                                }
                                mapFieldValue889 .put(key893, mapFieldValueValue895);
                            }
                            chunkLen890 = (decoder.mapNext());
                        } while (chunkLen890 > 0);
                    } else {
                        mapFieldValue889 = Collections.emptyMap();
                    }
                    mapField883 .put(key887, mapFieldValue889);
                }
                chunkLen884 = (decoder.mapNext());
            } while (chunkLen884 > 0);
        } else {
            mapField883 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadNestedMap.put(0, mapField883);
        return FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
    }

}
