
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

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedField_GenericDeserializer_3843748224527120400_2388689330760710730
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testNotRemoved908;
    private final Schema testNotRemoved2911;
    private final Schema subRecord913;
    private final Schema subRecordOptionSchema915;
    private final Schema testNotRemoved917;
    private final Schema testNotRemoved2920;
    private final Schema subRecordMap922;
    private final Schema subRecordArray928;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedField_GenericDeserializer_3843748224527120400_2388689330760710730(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testNotRemoved908 = readerSchema.getField("testNotRemoved").schema();
        this.testNotRemoved2911 = readerSchema.getField("testNotRemoved2").schema();
        this.subRecord913 = readerSchema.getField("subRecord").schema();
        this.subRecordOptionSchema915 = subRecord913 .getTypes().get(1);
        this.testNotRemoved917 = subRecordOptionSchema915 .getField("testNotRemoved").schema();
        this.testNotRemoved2920 = subRecordOptionSchema915 .getField("testNotRemoved2").schema();
        this.subRecordMap922 = readerSchema.getField("subRecordMap").schema();
        this.subRecordArray928 = readerSchema.getField("subRecordArray").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField907((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField907(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex909 = (decoder.readIndex());
        if (unionIndex909 == 0) {
            decoder.readNull();
        }
        if (unionIndex909 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(null));
            }
        }
        int unionIndex910 = (decoder.readIndex());
        if (unionIndex910 == 0) {
            decoder.readNull();
        }
        if (unionIndex910 == 1) {
            decoder.skipString();
        }
        int unionIndex912 = (decoder.readIndex());
        if (unionIndex912 == 0) {
            decoder.readNull();
        }
        if (unionIndex912 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(null));
            }
        }
        int unionIndex914 = (decoder.readIndex());
        if (unionIndex914 == 0) {
            decoder.readNull();
        }
        if (unionIndex914 == 1) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(2, deserializesubRecord916(FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(2), (decoder)));
        }
        Map<Utf8, IndexedRecord> subRecordMap923 = null;
        long chunkLen924 = (decoder.readMapStart());
        if (chunkLen924 > 0) {
            Map<Utf8, IndexedRecord> subRecordMapReuse925 = null;
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3) instanceof Map) {
                subRecordMapReuse925 = ((Map) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3));
            }
            if (subRecordMapReuse925 != (null)) {
                subRecordMapReuse925 .clear();
                subRecordMap923 = subRecordMapReuse925;
            } else {
                subRecordMap923 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter926 = 0; (counter926 <chunkLen924); counter926 ++) {
                    Utf8 key927 = (decoder.readString(null));
                    subRecordMap923 .put(key927, deserializesubRecord916(null, (decoder)));
                }
                chunkLen924 = (decoder.mapNext());
            } while (chunkLen924 > 0);
        } else {
            subRecordMap923 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(3, subRecordMap923);
        List<IndexedRecord> subRecordArray929 = null;
        long chunkLen930 = (decoder.readArrayStart());
        if (chunkLen930 > 0) {
            List<IndexedRecord> subRecordArrayReuse931 = null;
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof List) {
                subRecordArrayReuse931 = ((List) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4));
            }
            if (subRecordArrayReuse931 != (null)) {
                subRecordArrayReuse931 .clear();
                subRecordArray929 = subRecordArrayReuse931;
            } else {
                subRecordArray929 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen930), subRecordArray928);
            }
            do {
                for (int counter932 = 0; (counter932 <chunkLen930); counter932 ++) {
                    Object subRecordArrayArrayElementReuseVar933 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof GenericArray) {
                        subRecordArrayArrayElementReuseVar933 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4)).peek();
                    }
                    subRecordArray929 .add(deserializesubRecord916(subRecordArrayArrayElementReuseVar933, (decoder)));
                }
                chunkLen930 = (decoder.arrayNext());
            } while (chunkLen930 > 0);
        } else {
            subRecordArray929 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, subRecordArray928);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(4, subRecordArray929);
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
    }

    public IndexedRecord deserializesubRecord916(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecordOptionSchema915)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecordOptionSchema915);
        }
        int unionIndex918 = (decoder.readIndex());
        if (unionIndex918 == 0) {
            decoder.readNull();
        }
        if (unionIndex918 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        int unionIndex919 = (decoder.readIndex());
        if (unionIndex919 == 0) {
            decoder.readNull();
        }
        if (unionIndex919 == 1) {
            decoder.skipString();
        }
        int unionIndex921 = (decoder.readIndex());
        if (unionIndex921 == 0) {
            decoder.readNull();
        }
        if (unionIndex921 == 1) {
            if (subRecord.get(1) instanceof Utf8) {
                subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
            } else {
                subRecord.put(1, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
