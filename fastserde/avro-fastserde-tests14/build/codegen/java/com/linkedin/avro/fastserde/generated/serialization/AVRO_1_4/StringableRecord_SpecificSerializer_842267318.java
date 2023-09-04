
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
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
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
        serialize_StringableRecord0(data, (encoder));
        serialize_StringableRecord1(data, (encoder));
        serialize_StringableRecord2(data, (encoder));
        serialize_StringableRecord3(data, (encoder));
        serialize_StringableRecord4(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_StringableRecord0(StringableRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(1) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(1)));
        } else {
            (encoder).writeString(data.get(1).toString());
        }
        if (data.get(2) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(2)));
        } else {
            (encoder).writeString(data.get(2).toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_StringableRecord1(StringableRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(3) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(3)));
        } else {
            (encoder).writeString(data.get(3).toString());
        }
        if (data.get(4) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(4)));
        } else {
            (encoder).writeString(data.get(4).toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_StringableRecord2(StringableRecord data, Encoder encoder)
        throws IOException
    {
        List<CharSequence> urlArray0 = ((List<CharSequence> ) data.get(5));
        (encoder).writeArrayStart();
        if ((urlArray0 == null)||urlArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(urlArray0 .size());
            for (int counter0 = 0; (counter0 <urlArray0 .size()); counter0 ++) {
                (encoder).startItem();
                if (urlArray0 .get(counter0) instanceof Utf8) {
                    (encoder).writeString(((Utf8) urlArray0 .get(counter0)));
                } else {
                    (encoder).writeString(urlArray0 .get(counter0).toString());
                }
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, CharSequence> urlMap0 = ((Map<CharSequence, CharSequence> ) data.get(6));
        (encoder).writeMapStart();
        if ((urlMap0 == null)||urlMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(urlMap0 .size());
            for (CharSequence key0 : ((Map<CharSequence, CharSequence> ) urlMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                if (urlMap0 .get(key0) instanceof Utf8) {
                    (encoder).writeString(((Utf8) urlMap0 .get(key0)));
                } else {
                    (encoder).writeString(urlMap0 .get(key0).toString());
                }
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_StringableRecord3(StringableRecord data, Encoder encoder)
        throws IOException
    {
        StringableSubRecord subRecord0 = ((StringableSubRecord) data.get(7));
        serializeStringableSubRecord0(subRecord0, (encoder));
        AnotherSubRecord subRecordWithSubRecord0 = ((AnotherSubRecord) data.get(8));
        serializeAnotherSubRecord0(subRecordWithSubRecord0, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeStringableSubRecord0(StringableSubRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
        serialize_StringableSubRecord0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_StringableSubRecord0(StringableSubRecord data, Encoder encoder)
        throws IOException
    {
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

    @SuppressWarnings("unchecked")
    private void serialize_StringableRecord4(StringableRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence stringUnion0 = ((CharSequence) data.get(9));
        if (stringUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (stringUnion0 instanceof Utf8) {
                (encoder).writeString(((Utf8) stringUnion0));
            } else {
                (encoder).writeString(stringUnion0 .toString());
            }
        }
    }

}
