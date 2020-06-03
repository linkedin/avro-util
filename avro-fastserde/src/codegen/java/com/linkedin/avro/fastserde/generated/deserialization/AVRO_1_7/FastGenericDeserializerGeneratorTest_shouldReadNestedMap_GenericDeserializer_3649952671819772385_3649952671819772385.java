
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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

public class FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_3649952671819772385_3649952671819772385
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema mapField736;
    private final Schema mapFieldMapValueSchema742;
    private final Schema mapFieldValueMapValueSchema748;

    public FastGenericDeserializerGeneratorTest_shouldReadNestedMap_GenericDeserializer_3649952671819772385_3649952671819772385(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapField736 = readerSchema.getField("mapField").schema();
        this.mapFieldMapValueSchema742 = mapField736 .getValueType();
        this.mapFieldValueMapValueSchema748 = mapFieldMapValueSchema742 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap735((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedMap735(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadNestedMap = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, Map<Utf8, List<Integer>>> mapField737 = null;
        long chunkLen738 = (decoder.readMapStart());
        if (chunkLen738 > 0) {
            Map<Utf8, Map<Utf8, List<Integer>>> mapFieldReuse739 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0) instanceof Map) {
                mapFieldReuse739 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadNestedMap.get(0));
            }
            if (mapFieldReuse739 != (null)) {
                mapFieldReuse739 .clear();
                mapField737 = mapFieldReuse739;
            } else {
                mapField737 = new HashMap<Utf8, Map<Utf8, List<Integer>>>();
            }
            do {
                for (int counter740 = 0; (counter740 <chunkLen738); counter740 ++) {
                    Utf8 key741 = (decoder.readString(null));
                    Map<Utf8, List<Integer>> mapFieldValue743 = null;
                    long chunkLen744 = (decoder.readMapStart());
                    if (chunkLen744 > 0) {
                        Map<Utf8, List<Integer>> mapFieldValueReuse745 = null;
                        if (null instanceof Map) {
                            mapFieldValueReuse745 = ((Map) null);
                        }
                        if (mapFieldValueReuse745 != (null)) {
                            mapFieldValueReuse745 .clear();
                            mapFieldValue743 = mapFieldValueReuse745;
                        } else {
                            mapFieldValue743 = new HashMap<Utf8, List<Integer>>();
                        }
                        do {
                            for (int counter746 = 0; (counter746 <chunkLen744); counter746 ++) {
                                Utf8 key747 = (decoder.readString(null));
                                List<Integer> mapFieldValueValue749 = null;
                                long chunkLen750 = (decoder.readArrayStart());
                                if (chunkLen750 > 0) {
                                    List<Integer> mapFieldValueValueReuse751 = null;
                                    if (null instanceof List) {
                                        mapFieldValueValueReuse751 = ((List) null);
                                    }
                                    if (mapFieldValueValueReuse751 != (null)) {
                                        mapFieldValueValueReuse751 .clear();
                                        mapFieldValueValue749 = mapFieldValueValueReuse751;
                                    } else {
                                        mapFieldValueValue749 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen750), mapFieldValueMapValueSchema748);
                                    }
                                    do {
                                        for (int counter752 = 0; (counter752 <chunkLen750); counter752 ++) {
                                            Object mapFieldValueValueArrayElementReuseVar753 = null;
                                            if (null instanceof GenericArray) {
                                                mapFieldValueValueArrayElementReuseVar753 = ((GenericArray) null).peek();
                                            }
                                            mapFieldValueValue749 .add((decoder.readInt()));
                                        }
                                        chunkLen750 = (decoder.arrayNext());
                                    } while (chunkLen750 > 0);
                                } else {
                                    mapFieldValueValue749 = new org.apache.avro.generic.GenericData.Array<Integer>(0, mapFieldValueMapValueSchema748);
                                }
                                mapFieldValue743 .put(key747, mapFieldValueValue749);
                            }
                            chunkLen744 = (decoder.mapNext());
                        } while (chunkLen744 > 0);
                    } else {
                        mapFieldValue743 = Collections.emptyMap();
                    }
                    mapField737 .put(key741, mapFieldValue743);
                }
                chunkLen738 = (decoder.mapNext());
            } while (chunkLen738 > 0);
        } else {
            mapField737 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadNestedMap.put(0, mapField737);
        return FastGenericDeserializerGeneratorTest_shouldReadNestedMap;
    }

}
