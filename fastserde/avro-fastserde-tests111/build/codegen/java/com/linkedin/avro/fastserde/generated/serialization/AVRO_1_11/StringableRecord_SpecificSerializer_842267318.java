
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableSubRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class StringableRecord_SpecificSerializer_842267318
    implements FastSerializer<StringableRecord>
{


    public void serialize(StringableRecord data, Encoder encoder)
        throws IOException
    {
        serializeStringableRecord0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeStringableRecord0(StringableRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeString(data.get(0).toString());
        (encoder).writeString(data.get(1).toString());
        (encoder).writeString(data.get(2).toString());
        (encoder).writeString(data.get(3).toString());
        (encoder).writeString(data.get(4).toString());
        List<URL> urlArray0 = ((List<URL> ) data.get(5));
        (encoder).writeArrayStart();
        if ((urlArray0 == null)||urlArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(urlArray0 .size());
            for (int counter0 = 0; (counter0 <urlArray0 .size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeString(urlArray0 .get(counter0).toString());
            }
        }
        (encoder).writeArrayEnd();
        Map<URL, BigInteger> urlMap0 = ((Map<URL, BigInteger> ) data.get(6));
        (encoder).writeMapStart();
        if ((urlMap0 == null)||urlMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(urlMap0 .size());
            for (URL key0 : ((Map<URL, BigInteger> ) urlMap0).keySet()) {
                (encoder).startItem();
                String keyString0 = key0 .toString();
                (encoder).writeString(keyString0);
                (encoder).writeString(urlMap0 .get(key0).toString());
            }
        }
        (encoder).writeMapEnd();
        StringableSubRecord subRecord0 = ((StringableSubRecord) data.get(7));
        serializeStringableSubRecord0(subRecord0, (encoder));
        AnotherSubRecord subRecordWithSubRecord0 = ((AnotherSubRecord) data.get(8));
        serializeAnotherSubRecord0(subRecordWithSubRecord0, (encoder));
        String stringUnion0 = ((String) data.get(9));
        if (stringUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeString(stringUnion0);
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeStringableSubRecord0(StringableSubRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeString(data.get(0).toString());
        Object nullStringIntUnion0 = ((Object) data.get(1));
        if (nullStringIntUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullStringIntUnion0 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (nullStringIntUnion0 instanceof Utf8) {
                    (encoder).writeString(((Utf8) nullStringIntUnion0));
                } else {
                    (encoder).writeString(nullStringIntUnion0 .toString());
                }
            } else {
                if (nullStringIntUnion0 instanceof Integer) {
                    (encoder).writeIndex(2);
                    (encoder).writeInt(((Integer) nullStringIntUnion0));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeAnotherSubRecord0(AnotherSubRecord data, Encoder encoder)
        throws IOException
    {
        StringableSubRecord subRecord1 = ((StringableSubRecord) data.get(0));
        serializeStringableSubRecord0(subRecord1, (encoder));
    }

}
