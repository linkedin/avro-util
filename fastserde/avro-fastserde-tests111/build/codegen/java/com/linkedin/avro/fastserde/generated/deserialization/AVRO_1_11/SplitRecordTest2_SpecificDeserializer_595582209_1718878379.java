
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.FullRecord;
import com.linkedin.avro.fastserde.generated.avro.StringRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class SplitRecordTest2_SpecificDeserializer_595582209_1718878379
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2>
{

    private final Schema readerSchema;

    public SplitRecordTest2_SpecificDeserializer_595582209_1718878379(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2 deserialize(com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2 reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeSplitRecordTest20((reuse), (decoder), (customization));
    }

    public com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2 deserializeSplitRecordTest20(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2 SplitRecordTest2;
        if ((reuse)!= null) {
            SplitRecordTest2 = ((com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2)(reuse));
        } else {
            SplitRecordTest2 = new com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2();
        }
        SplitRecordTest2 .put(0, deserializeStringRecord0(SplitRecordTest2 .get(0), (decoder), (customization)));
        populate_SplitRecordTest20((SplitRecordTest2), (customization), (decoder));
        return SplitRecordTest2;
    }

    public StringRecord deserializeStringRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        StringRecord IntRecord;
        if ((reuse)!= null) {
            IntRecord = ((StringRecord)(reuse));
        } else {
            IntRecord = new StringRecord();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            IntRecord.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = IntRecord.get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                IntRecord.put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex0));
            }
        }
        populate_IntRecord0((IntRecord), (customization), (decoder));
        return IntRecord;
    }

    private void populate_IntRecord0(StringRecord IntRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                decoder.readInt();
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex1));
            }
        }
    }

    private void populate_SplitRecordTest20(com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2 SplitRecordTest2, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        SplitRecordTest2 .put(1, deserializeIntRecord0(SplitRecordTest2 .get(1), (decoder), (customization)));
        List<FullRecord> record30 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = SplitRecordTest2 .get(2);
        if (oldArray0 instanceof List) {
            record30 = ((List) oldArray0);
            if (record30 instanceof GenericArray) {
                ((GenericArray) record30).reset();
            } else {
                record30 .clear();
            }
        } else {
            record30 = new ArrayList<FullRecord>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object record3ArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    record3ArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                record30 .add(deserializeFullRecord0(record3ArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        SplitRecordTest2 .put(2, record30);
    }

    public com.linkedin.avro.fastserde.generated.avro.IntRecord deserializeIntRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.IntRecord IntRecord;
        if ((reuse)!= null) {
            IntRecord = ((com.linkedin.avro.fastserde.generated.avro.IntRecord)(reuse));
        } else {
            IntRecord = new com.linkedin.avro.fastserde.generated.avro.IntRecord();
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex2 == 1) {
                decoder.skipString();
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex2));
            }
        }
        populate_IntRecord1((IntRecord), (customization), (decoder));
        return IntRecord;
    }

    private void populate_IntRecord1(com.linkedin.avro.fastserde.generated.avro.IntRecord IntRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            IntRecord.put(0, null);
        } else {
            if (unionIndex3 == 1) {
                IntRecord.put(0, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex3));
            }
        }
    }

    public FullRecord deserializeFullRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        FullRecord IntRecord;
        if ((reuse)!= null) {
            IntRecord = ((FullRecord)(reuse));
        } else {
            IntRecord = new FullRecord();
        }
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            IntRecord.put(0, null);
        } else {
            if (unionIndex4 == 1) {
                Utf8 charSequence1;
                Object oldString1 = IntRecord.get(0);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                IntRecord.put(0, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex4));
            }
        }
        populate_IntRecord2((IntRecord), (customization), (decoder));
        return IntRecord;
    }

    private void populate_IntRecord2(FullRecord IntRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            IntRecord.put(1, null);
        } else {
            if (unionIndex5 == 1) {
                IntRecord.put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex5));
            }
        }
    }

}
