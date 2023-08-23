
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class SplitRecordTest1_SpecificDeserializer_1718878379_595582209
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1>
{

    private final Schema readerSchema;

    public SplitRecordTest1_SpecificDeserializer_1718878379_595582209(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1 deserialize(com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1 reuse, Decoder decoder)
        throws IOException
    {
        return deserializeSplitRecordTest10((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1 deserializeSplitRecordTest10(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1 SplitRecordTest1;
        if ((reuse)!= null) {
            SplitRecordTest1 = ((com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1)(reuse));
        } else {
            SplitRecordTest1 = new com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1();
        }
        SplitRecordTest1 .put(0, deserializeFullRecord0(SplitRecordTest1 .get(0), (decoder)));
        populate_SplitRecordTest10((SplitRecordTest1), (decoder));
        return SplitRecordTest1;
    }

    public com.linkedin.avro.fastserde.generated.avro.FullRecord deserializeFullRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FullRecord FullRecord;
        if ((reuse)!= null) {
            FullRecord = ((com.linkedin.avro.fastserde.generated.avro.FullRecord)(reuse));
        } else {
            FullRecord = new com.linkedin.avro.fastserde.generated.avro.FullRecord();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FullRecord.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Object oldString0 = FullRecord.get(0);
                if (oldString0 instanceof Utf8) {
                    FullRecord.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    FullRecord.put(0, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex0));
            }
        }
        FullRecord.put(1, null);
        return FullRecord;
    }

    private void populate_SplitRecordTest10(com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1 SplitRecordTest1, Decoder decoder)
        throws IOException
    {
        SplitRecordTest1 .put(1, deserializeFullRecord1(SplitRecordTest1 .get(1), (decoder)));
        List<com.linkedin.avro.fastserde.generated.avro.FullRecord> record30 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = SplitRecordTest1 .get(2);
        if (oldArray0 instanceof List) {
            record30 = ((List) oldArray0);
            record30 .clear();
        } else {
            record30 = new ArrayList<com.linkedin.avro.fastserde.generated.avro.FullRecord>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object record3ArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    record3ArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                record30 .add(deserializeFullRecord2(record3ArrayElementReuseVar0, (decoder)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        SplitRecordTest1 .put(2, record30);
    }

    public com.linkedin.avro.fastserde.generated.avro.FullRecord deserializeFullRecord1(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FullRecord FullRecord;
        if ((reuse)!= null) {
            FullRecord = ((com.linkedin.avro.fastserde.generated.avro.FullRecord)(reuse));
        } else {
            FullRecord = new com.linkedin.avro.fastserde.generated.avro.FullRecord();
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            FullRecord.put(1, null);
        } else {
            if (unionIndex1 == 1) {
                FullRecord.put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex1));
            }
        }
        FullRecord.put(0, null);
        return FullRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.FullRecord deserializeFullRecord2(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FullRecord FullRecord;
        if ((reuse)!= null) {
            FullRecord = ((com.linkedin.avro.fastserde.generated.avro.FullRecord)(reuse));
        } else {
            FullRecord = new com.linkedin.avro.fastserde.generated.avro.FullRecord();
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            FullRecord.put(0, null);
        } else {
            if (unionIndex2 == 1) {
                Object oldString1 = FullRecord.get(0);
                if (oldString1 instanceof Utf8) {
                    FullRecord.put(0, (decoder).readString(((Utf8) oldString1)));
                } else {
                    FullRecord.put(0, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex2));
            }
        }
        populate_FullRecord0((FullRecord), (decoder));
        return FullRecord;
    }

    private void populate_FullRecord0(com.linkedin.avro.fastserde.generated.avro.FullRecord FullRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            FullRecord.put(1, null);
        } else {
            if (unionIndex3 == 1) {
                FullRecord.put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex3));
            }
        }
    }

}
