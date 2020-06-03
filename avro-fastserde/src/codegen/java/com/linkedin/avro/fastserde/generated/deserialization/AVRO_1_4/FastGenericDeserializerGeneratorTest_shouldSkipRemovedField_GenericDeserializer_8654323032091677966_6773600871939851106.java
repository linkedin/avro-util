
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

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedField_GenericDeserializer_8654323032091677966_6773600871939851106
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testNotRemoved1054;
    private final Schema testNotRemoved21057;
    private final Schema subRecord1059;
    private final Schema subRecordOptionSchema1061;
    private final Schema testNotRemoved1063;
    private final Schema testNotRemoved21066;
    private final Schema subRecordMap1068;
    private final Schema subRecordArray1074;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedField_GenericDeserializer_8654323032091677966_6773600871939851106(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testNotRemoved1054 = readerSchema.getField("testNotRemoved").schema();
        this.testNotRemoved21057 = readerSchema.getField("testNotRemoved2").schema();
        this.subRecord1059 = readerSchema.getField("subRecord").schema();
        this.subRecordOptionSchema1061 = subRecord1059 .getTypes().get(1);
        this.testNotRemoved1063 = subRecordOptionSchema1061 .getField("testNotRemoved").schema();
        this.testNotRemoved21066 = subRecordOptionSchema1061 .getField("testNotRemoved2").schema();
        this.subRecordMap1068 = readerSchema.getField("subRecordMap").schema();
        this.subRecordArray1074 = readerSchema.getField("subRecordArray").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField1053((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField1053(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex1055 = (decoder.readIndex());
        if (unionIndex1055 == 0) {
            decoder.readNull();
        }
        if (unionIndex1055 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(null));
            }
        }
        int unionIndex1056 = (decoder.readIndex());
        if (unionIndex1056 == 0) {
            decoder.readNull();
        }
        if (unionIndex1056 == 1) {
            decoder.skipString();
        }
        int unionIndex1058 = (decoder.readIndex());
        if (unionIndex1058 == 0) {
            decoder.readNull();
        }
        if (unionIndex1058 == 1) {
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1) instanceof Utf8) {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1))));
            } else {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(null));
            }
        }
        int unionIndex1060 = (decoder.readIndex());
        if (unionIndex1060 == 0) {
            decoder.readNull();
        }
        if (unionIndex1060 == 1) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(2, deserializesubRecord1062(FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(2), (decoder)));
        }
        Map<Utf8, IndexedRecord> subRecordMap1069 = null;
        long chunkLen1070 = (decoder.readMapStart());
        if (chunkLen1070 > 0) {
            Map<Utf8, IndexedRecord> subRecordMapReuse1071 = null;
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3) instanceof Map) {
                subRecordMapReuse1071 = ((Map) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3));
            }
            if (subRecordMapReuse1071 != (null)) {
                subRecordMapReuse1071 .clear();
                subRecordMap1069 = subRecordMapReuse1071;
            } else {
                subRecordMap1069 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter1072 = 0; (counter1072 <chunkLen1070); counter1072 ++) {
                    Utf8 key1073 = (decoder.readString(null));
                    subRecordMap1069 .put(key1073, deserializesubRecord1062(null, (decoder)));
                }
                chunkLen1070 = (decoder.mapNext());
            } while (chunkLen1070 > 0);
        } else {
            subRecordMap1069 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(3, subRecordMap1069);
        List<IndexedRecord> subRecordArray1075 = null;
        long chunkLen1076 = (decoder.readArrayStart());
        if (chunkLen1076 > 0) {
            List<IndexedRecord> subRecordArrayReuse1077 = null;
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof List) {
                subRecordArrayReuse1077 = ((List) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4));
            }
            if (subRecordArrayReuse1077 != (null)) {
                subRecordArrayReuse1077 .clear();
                subRecordArray1075 = subRecordArrayReuse1077;
            } else {
                subRecordArray1075 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen1076), subRecordArray1074);
            }
            do {
                for (int counter1078 = 0; (counter1078 <chunkLen1076); counter1078 ++) {
                    Object subRecordArrayArrayElementReuseVar1079 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof GenericArray) {
                        subRecordArrayArrayElementReuseVar1079 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4)).peek();
                    }
                    subRecordArray1075 .add(deserializesubRecord1062(subRecordArrayArrayElementReuseVar1079, (decoder)));
                }
                chunkLen1076 = (decoder.arrayNext());
            } while (chunkLen1076 > 0);
        } else {
            subRecordArray1075 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, subRecordArray1074);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(4, subRecordArray1075);
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
    }

    public IndexedRecord deserializesubRecord1062(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecordOptionSchema1061)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecordOptionSchema1061);
        }
        int unionIndex1064 = (decoder.readIndex());
        if (unionIndex1064 == 0) {
            decoder.readNull();
        }
        if (unionIndex1064 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        int unionIndex1065 = (decoder.readIndex());
        if (unionIndex1065 == 0) {
            decoder.readNull();
        }
        if (unionIndex1065 == 1) {
            decoder.skipString();
        }
        int unionIndex1067 = (decoder.readIndex());
        if (unionIndex1067 == 0) {
            decoder.readNull();
        }
        if (unionIndex1067 == 1) {
            if (subRecord.get(1) instanceof Utf8) {
                subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
            } else {
                subRecord.put(1, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
