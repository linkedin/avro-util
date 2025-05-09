
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.FullRecord;
import com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class SplitRecordTest1_SpecificDeserializer_1718878379_595582209
    implements FastDeserializer<SplitRecordTest1>
{

    private final Schema readerSchema;

    public SplitRecordTest1_SpecificDeserializer_1718878379_595582209(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public SplitRecordTest1 deserialize(SplitRecordTest1 reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeSplitRecordTest10((reuse), (decoder), (customization));
    }

    public SplitRecordTest1 deserializeSplitRecordTest10(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        SplitRecordTest1 splitRecordTest10;
        if ((reuse)!= null) {
            splitRecordTest10 = ((SplitRecordTest1)(reuse));
        } else {
            splitRecordTest10 = new SplitRecordTest1();
        }
        splitRecordTest10 .put(0, deserializeFullRecord0(splitRecordTest10 .get(0), (decoder), (customization)));
        populate_SplitRecordTest10((splitRecordTest10), (customization), (decoder));
        return splitRecordTest10;
    }

    public FullRecord deserializeFullRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        FullRecord fullRecord0;
        if ((reuse)!= null) {
            fullRecord0 = ((FullRecord)(reuse));
        } else {
            fullRecord0 = new FullRecord();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fullRecord0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = fullRecord0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                fullRecord0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex0));
            }
        }
        fullRecord0 .put(1, null);
        return fullRecord0;
    }

    private void populate_SplitRecordTest10(SplitRecordTest1 splitRecordTest10, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        splitRecordTest10 .put(1, deserializeFullRecord1(splitRecordTest10 .get(1), (decoder), (customization)));
        List<FullRecord> record30 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = splitRecordTest10 .get(2);
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
                record30 .add(deserializeFullRecord2(record3ArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        splitRecordTest10 .put(2, record30);
    }

    public FullRecord deserializeFullRecord1(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        FullRecord fullRecord1;
        if ((reuse)!= null) {
            fullRecord1 = ((FullRecord)(reuse));
        } else {
            fullRecord1 = new FullRecord();
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            fullRecord1 .put(1, null);
        } else {
            if (unionIndex1 == 1) {
                fullRecord1 .put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex1));
            }
        }
        fullRecord1 .put(0, null);
        return fullRecord1;
    }

    public FullRecord deserializeFullRecord2(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        FullRecord fullRecord2;
        if ((reuse)!= null) {
            fullRecord2 = ((FullRecord)(reuse));
        } else {
            fullRecord2 = new FullRecord();
        }
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            fullRecord2 .put(0, null);
        } else {
            if (unionIndex2 == 1) {
                Utf8 charSequence1;
                Object oldString1 = fullRecord2 .get(0);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                fullRecord2 .put(0, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'field1': "+ unionIndex2));
            }
        }
        populate_FullRecord0((fullRecord2), (customization), (decoder));
        return fullRecord2;
    }

    private void populate_FullRecord0(FullRecord fullRecord2, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            fullRecord2 .put(1, null);
        } else {
            if (unionIndex3 == 1) {
                fullRecord2 .put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'field2': "+ unionIndex3));
            }
        }
    }

}
