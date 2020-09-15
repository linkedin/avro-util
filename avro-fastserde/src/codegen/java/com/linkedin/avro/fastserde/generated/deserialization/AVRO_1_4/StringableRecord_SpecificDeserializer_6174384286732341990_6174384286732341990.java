
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class StringableRecord_SpecificDeserializer_6174384286732341990_6174384286732341990
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.StringableRecord>
{

    private final Schema readerSchema;

    public StringableRecord_SpecificDeserializer_6174384286732341990_6174384286732341990(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserialize(com.linkedin.avro.fastserde.generated.avro.StringableRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeStringableRecord0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserializeStringableRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord;
        if ((reuse)!= null) {
            StringableRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableRecord)(reuse));
        } else {
            StringableRecord = new com.linkedin.avro.fastserde.generated.avro.StringableRecord();
        }
        Object oldString0 = StringableRecord.get(0);
        if (oldString0 instanceof Utf8) {
            StringableRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            StringableRecord.put(0, (decoder).readString(null));
        }
        Object oldString1 = StringableRecord.get(1);
        if (oldString1 instanceof Utf8) {
            StringableRecord.put(1, (decoder).readString(((Utf8) oldString1)));
        } else {
            StringableRecord.put(1, (decoder).readString(null));
        }
        Object oldString2 = StringableRecord.get(2);
        if (oldString2 instanceof Utf8) {
            StringableRecord.put(2, (decoder).readString(((Utf8) oldString2)));
        } else {
            StringableRecord.put(2, (decoder).readString(null));
        }
        Object oldString3 = StringableRecord.get(3);
        if (oldString3 instanceof Utf8) {
            StringableRecord.put(3, (decoder).readString(((Utf8) oldString3)));
        } else {
            StringableRecord.put(3, (decoder).readString(null));
        }
        Object oldString4 = StringableRecord.get(4);
        if (oldString4 instanceof Utf8) {
            StringableRecord.put(4, (decoder).readString(((Utf8) oldString4)));
        } else {
            StringableRecord.put(4, (decoder).readString(null));
        }
        List<Utf8> urlArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = StringableRecord.get(5);
        if (oldArray0 instanceof List) {
            urlArray0 = ((List) oldArray0);
            urlArray0 .clear();
        } else {
            urlArray0 = new ArrayList<Utf8>();
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object urlArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    urlArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                if (urlArrayArrayElementReuseVar0 instanceof Utf8) {
                    urlArray0 .add((decoder).readString(((Utf8) urlArrayArrayElementReuseVar0)));
                } else {
                    urlArray0 .add((decoder).readString(null));
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        StringableRecord.put(5, urlArray0);
        Map<Utf8, Utf8> urlMap0 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<Utf8, Utf8> urlMapReuse0 = null;
            Object oldMap0 = StringableRecord.get(6);
            if (oldMap0 instanceof Map) {
                urlMapReuse0 = ((Map) oldMap0);
            }
            if (urlMapReuse0 != (null)) {
                urlMapReuse0 .clear();
                urlMap0 = urlMapReuse0;
            } else {
                urlMap0 = new HashMap<Utf8, Utf8>();
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    urlMap0 .put(key0, (decoder).readString(null));
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            urlMap0 = Collections.emptyMap();
        }
        StringableRecord.put(6, urlMap0);
        StringableRecord.put(7, deserializeStringableSubRecord0(StringableRecord.get(7), (decoder)));
        StringableRecord.put(8, deserializeAnotherSubRecord0(StringableRecord.get(8), (decoder)));
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString6 = StringableRecord.get(9);
                if (oldString6 instanceof Utf8) {
                    StringableRecord.put(9, (decoder).readString(((Utf8) oldString6)));
                } else {
                    StringableRecord.put(9, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'stringUnion': "+ unionIndex0));
        }
        return StringableRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord deserializeStringableSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableSubRecord StringableSubRecord;
        if ((reuse)!= null) {
            StringableSubRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableSubRecord)(reuse));
        } else {
            StringableSubRecord = new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord();
        }
        Object oldString5 = StringableSubRecord.get(0);
        if (oldString5 instanceof Utf8) {
            StringableSubRecord.put(0, (decoder).readString(((Utf8) oldString5)));
        } else {
            StringableSubRecord.put(0, (decoder).readString(null));
        }
        return StringableSubRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord deserializeAnotherSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord AnotherSubRecord;
        if ((reuse)!= null) {
            AnotherSubRecord = ((com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord)(reuse));
        } else {
            AnotherSubRecord = new com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord();
        }
        AnotherSubRecord.put(0, deserializeStringableSubRecord0(AnotherSubRecord.get(0), (decoder)));
        return AnotherSubRecord;
    }

}
