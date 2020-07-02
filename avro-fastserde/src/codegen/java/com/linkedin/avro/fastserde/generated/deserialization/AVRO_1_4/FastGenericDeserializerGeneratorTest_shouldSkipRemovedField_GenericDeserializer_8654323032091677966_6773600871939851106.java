
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
    private final Schema testNotRemoved0;
    private final Schema testNotRemoved20;
    private final Schema subRecord0;
    private final Schema subRecordOptionSchema0;
    private final Schema testNotRemoved1;
    private final Schema testNotRemoved21;
    private final Schema subRecordMap0;
    private final Schema subRecordArray0;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedField_GenericDeserializer_8654323032091677966_6773600871939851106(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testNotRemoved0 = readerSchema.getField("testNotRemoved").schema();
        this.testNotRemoved20 = readerSchema.getField("testNotRemoved2").schema();
        this.subRecord0 = readerSchema.getField("subRecord").schema();
        this.subRecordOptionSchema0 = subRecord0 .getTypes().get(1);
        this.testNotRemoved1 = subRecordOptionSchema0 .getField("testNotRemoved").schema();
        this.testNotRemoved21 = subRecordOptionSchema0 .getField("testNotRemoved2").schema();
        this.subRecordMap0 = readerSchema.getField("subRecordMap").schema();
        this.subRecordArray0 = readerSchema.getField("subRecordArray").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedField0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0) instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(0))));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(0, (decoder).readString(null));
                }
            }
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                decoder.skipString();
            }
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex2 == 1) {
                if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1) instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(1))));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(1, (decoder).readString(null));
                }
            }
        }
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex3 == 1) {
                FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(2, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(2), (decoder)));
            }
        }
        Map<Utf8, IndexedRecord> subRecordMap1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            Map<Utf8, IndexedRecord> subRecordMapReuse0 = null;
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3) instanceof Map) {
                subRecordMapReuse0 = ((Map) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(3));
            }
            if (subRecordMapReuse0 != (null)) {
                subRecordMapReuse0 .clear();
                subRecordMap1 = subRecordMapReuse0;
            } else {
                subRecordMap1 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    subRecordMap1 .put(key0, deserializesubRecord0(null, (decoder)));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            subRecordMap1 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(3, subRecordMap1);
        List<IndexedRecord> subRecordArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        if (chunkLen1 > 0) {
            if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof List) {
                subRecordArray1 = ((List) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4));
                subRecordArray1 .clear();
            } else {
                subRecordArray1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen1), subRecordArray0);
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Object subRecordArrayArrayElementReuseVar0 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4) instanceof GenericArray) {
                        subRecordArrayArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.get(4)).peek();
                    }
                    subRecordArray1 .add(deserializesubRecord0(subRecordArrayArrayElementReuseVar0, (decoder)));
                }
                chunkLen1 = (decoder.arrayNext());
            } while (chunkLen1 > 0);
        } else {
            subRecordArray1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen1), subRecordArray0);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedField.put(4, subRecordArray1);
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedField;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecordOptionSchema0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecordOptionSchema0);
        }
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex4 == 1) {
                if (subRecord.get(0) instanceof Utf8) {
                    subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
                } else {
                    subRecord.put(0, (decoder).readString(null));
                }
            }
        }
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex5 == 1) {
                decoder.skipString();
            }
        }
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex6 == 1) {
                if (subRecord.get(1) instanceof Utf8) {
                    subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
                } else {
                    subRecord.put(1, (decoder).readString(null));
                }
            }
        }
        return subRecord;
    }

}
