
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

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3518250962527328123_3518250962527328123
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordsArray945;
    private final Schema recordsArrayArrayElemSchema950;
    private final Schema subField953;
    private final Schema recordsMap955;
    private final Schema recordsArrayUnion961;
    private final Schema recordsArrayUnionOptionSchema963;
    private final Schema recordsArrayUnionOptionArrayElemSchema968;
    private final Schema recordsMapUnion971;
    private final Schema recordsMapUnionOptionSchema973;
    private final Schema recordsMapUnionOptionMapValueSchema979;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3518250962527328123_3518250962527328123(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArray945 = readerSchema.getField("recordsArray").schema();
        this.recordsArrayArrayElemSchema950 = recordsArray945 .getElementType();
        this.subField953 = recordsArrayArrayElemSchema950 .getField("subField").schema();
        this.recordsMap955 = readerSchema.getField("recordsMap").schema();
        this.recordsArrayUnion961 = readerSchema.getField("recordsArrayUnion").schema();
        this.recordsArrayUnionOptionSchema963 = recordsArrayUnion961 .getTypes().get(1);
        this.recordsArrayUnionOptionArrayElemSchema968 = recordsArrayUnionOptionSchema963 .getElementType();
        this.recordsMapUnion971 = readerSchema.getField("recordsMapUnion").schema();
        this.recordsMapUnionOptionSchema973 = recordsMapUnion971 .getTypes().get(1);
        this.recordsMapUnionOptionMapValueSchema979 = recordsMapUnionOptionSchema973 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField944((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField944(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<IndexedRecord> recordsArray946 = null;
        long chunkLen947 = (decoder.readArrayStart());
        if (chunkLen947 > 0) {
            List<IndexedRecord> recordsArrayReuse948 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0) instanceof List) {
                recordsArrayReuse948 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0));
            }
            if (recordsArrayReuse948 != (null)) {
                recordsArrayReuse948 .clear();
                recordsArray946 = recordsArrayReuse948;
            } else {
                recordsArray946 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen947), recordsArray945);
            }
            do {
                for (int counter949 = 0; (counter949 <chunkLen947); counter949 ++) {
                    Object recordsArrayArrayElementReuseVar951 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0) instanceof GenericArray) {
                        recordsArrayArrayElementReuseVar951 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0)).peek();
                    }
                    recordsArray946 .add(deserializesubRecord952(recordsArrayArrayElementReuseVar951, (decoder)));
                }
                chunkLen947 = (decoder.arrayNext());
            } while (chunkLen947 > 0);
        } else {
            recordsArray946 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsArray945);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(0, recordsArray946);
        Map<Utf8, IndexedRecord> recordsMap956 = null;
        long chunkLen957 = (decoder.readMapStart());
        if (chunkLen957 > 0) {
            Map<Utf8, IndexedRecord> recordsMapReuse958 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(1) instanceof Map) {
                recordsMapReuse958 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(1));
            }
            if (recordsMapReuse958 != (null)) {
                recordsMapReuse958 .clear();
                recordsMap956 = recordsMapReuse958;
            } else {
                recordsMap956 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter959 = 0; (counter959 <chunkLen957); counter959 ++) {
                    Utf8 key960 = (decoder.readString(null));
                    recordsMap956 .put(key960, deserializesubRecord952(null, (decoder)));
                }
                chunkLen957 = (decoder.mapNext());
            } while (chunkLen957 > 0);
        } else {
            recordsMap956 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(1, recordsMap956);
        int unionIndex962 = (decoder.readIndex());
        if (unionIndex962 == 0) {
            decoder.readNull();
        }
        if (unionIndex962 == 1) {
            List<IndexedRecord> recordsArrayUnionOption964 = null;
            long chunkLen965 = (decoder.readArrayStart());
            if (chunkLen965 > 0) {
                List<IndexedRecord> recordsArrayUnionOptionReuse966 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2) instanceof List) {
                    recordsArrayUnionOptionReuse966 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2));
                }
                if (recordsArrayUnionOptionReuse966 != (null)) {
                    recordsArrayUnionOptionReuse966 .clear();
                    recordsArrayUnionOption964 = recordsArrayUnionOptionReuse966;
                } else {
                    recordsArrayUnionOption964 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen965), recordsArrayUnionOptionSchema963);
                }
                do {
                    for (int counter967 = 0; (counter967 <chunkLen965); counter967 ++) {
                        Object recordsArrayUnionOptionArrayElementReuseVar969 = null;
                        if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2) instanceof GenericArray) {
                            recordsArrayUnionOptionArrayElementReuseVar969 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2)).peek();
                        }
                        int unionIndex970 = (decoder.readIndex());
                        if (unionIndex970 == 0) {
                            decoder.readNull();
                        }
                        if (unionIndex970 == 1) {
                            recordsArrayUnionOption964 .add(deserializesubRecord952(recordsArrayUnionOptionArrayElementReuseVar969, (decoder)));
                        }
                    }
                    chunkLen965 = (decoder.arrayNext());
                } while (chunkLen965 > 0);
            } else {
                recordsArrayUnionOption964 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsArrayUnionOptionSchema963);
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(2, recordsArrayUnionOption964);
        }
        int unionIndex972 = (decoder.readIndex());
        if (unionIndex972 == 0) {
            decoder.readNull();
        }
        if (unionIndex972 == 1) {
            Map<Utf8, IndexedRecord> recordsMapUnionOption974 = null;
            long chunkLen975 = (decoder.readMapStart());
            if (chunkLen975 > 0) {
                Map<Utf8, IndexedRecord> recordsMapUnionOptionReuse976 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(3) instanceof Map) {
                    recordsMapUnionOptionReuse976 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(3));
                }
                if (recordsMapUnionOptionReuse976 != (null)) {
                    recordsMapUnionOptionReuse976 .clear();
                    recordsMapUnionOption974 = recordsMapUnionOptionReuse976;
                } else {
                    recordsMapUnionOption974 = new HashMap<Utf8, IndexedRecord>();
                }
                do {
                    for (int counter977 = 0; (counter977 <chunkLen975); counter977 ++) {
                        Utf8 key978 = (decoder.readString(null));
                        int unionIndex980 = (decoder.readIndex());
                        if (unionIndex980 == 0) {
                            decoder.readNull();
                        }
                        if (unionIndex980 == 1) {
                            recordsMapUnionOption974 .put(key978, deserializesubRecord952(null, (decoder)));
                        }
                    }
                    chunkLen975 = (decoder.mapNext());
                } while (chunkLen975 > 0);
            } else {
                recordsMapUnionOption974 = Collections.emptyMap();
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(3, recordsMapUnionOption974);
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
    }

    public IndexedRecord deserializesubRecord952(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayArrayElemSchema950)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayArrayElemSchema950);
        }
        int unionIndex954 = (decoder.readIndex());
        if (unionIndex954 == 0) {
            decoder.readNull();
        }
        if (unionIndex954 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
