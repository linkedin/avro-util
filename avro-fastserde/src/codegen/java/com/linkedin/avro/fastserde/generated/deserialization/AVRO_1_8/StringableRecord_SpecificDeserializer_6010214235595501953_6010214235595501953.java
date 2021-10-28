
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class StringableRecord_SpecificDeserializer_6010214235595501953_6010214235595501953
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.StringableRecord>
{

    private final Schema readerSchema;

    public StringableRecord_SpecificDeserializer_6010214235595501953_6010214235595501953(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserialize(com.linkedin.avro.fastserde.generated.avro.StringableRecord reuse, Decoder decoder)
        throws IOException
    {
        try {
            return deserializeStringableRecord0((reuse), (decoder));
        } catch (NumberFormatException e) {
            throw new AvroRuntimeException(e);
        } catch (MalformedURLException e) {
            throw new AvroRuntimeException(e);
        } catch (URISyntaxException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserializeStringableRecord0(Object reuse, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord;
        if ((reuse)!= null) {
            StringableRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableRecord)(reuse));
        } else {
            StringableRecord = new com.linkedin.avro.fastserde.generated.avro.StringableRecord();
        }
        StringableRecord.put(0, new BigInteger((decoder.readString())));
        StringableRecord.put(1, new BigDecimal((decoder.readString())));
        StringableRecord.put(2, new URI((decoder.readString())));
        StringableRecord.put(3, new URL((decoder.readString())));
        StringableRecord.put(4, new File((decoder.readString())));
        List<URL> urlArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = StringableRecord.get(5);
        if (oldArray0 instanceof List) {
            urlArray0 = ((List) oldArray0);
            urlArray0 .clear();
        } else {
            urlArray0 = new ArrayList<URL>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object urlArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    urlArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                urlArray0 .add(new URL((decoder.readString())));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        StringableRecord.put(5, urlArray0);
        Map<URL, BigInteger> urlMap0 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<URL, BigInteger> urlMapReuse0 = null;
            Object oldMap0 = StringableRecord.get(6);
            if (oldMap0 instanceof Map) {
                urlMapReuse0 = ((Map) oldMap0);
            }
            if (urlMapReuse0 != (null)) {
                urlMapReuse0 .clear();
                urlMap0 = urlMapReuse0;
            } else {
                urlMap0 = new HashMap<URL, BigInteger>(((int)(((chunkLen1 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    URL key0 = new URL((decoder.readString()));
                    urlMap0 .put(key0, new BigInteger((decoder.readString())));
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            urlMap0 = new HashMap<URL, BigInteger>(0);
        }
        StringableRecord.put(6, urlMap0);
        StringableRecord.put(7, deserializeStringableSubRecord0(StringableRecord.get(7), (decoder)));
        StringableRecord.put(8, deserializeAnotherSubRecord0(StringableRecord.get(8), (decoder)));
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                StringableRecord.put(9, (decoder).readString());
            } else {
                throw new RuntimeException(("Illegal union index for 'stringUnion': "+ unionIndex1));
            }
        }
        return StringableRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord deserializeStringableSubRecord0(Object reuse, Decoder decoder)
        throws IOException, URISyntaxException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableSubRecord StringableSubRecord;
        if ((reuse)!= null) {
            StringableSubRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableSubRecord)(reuse));
        } else {
            StringableSubRecord = new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord();
        }
        StringableSubRecord.put(0, new URI((decoder.readString())));
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                Object oldString0 = StringableSubRecord.get(1);
                if (oldString0 instanceof Utf8) {
                    StringableSubRecord.put(1, (decoder).readString(((Utf8) oldString0)));
                } else {
                    StringableSubRecord.put(1, (decoder).readString(null));
                }
            } else {
                if (unionIndex0 == 2) {
                    StringableSubRecord.put(1, (decoder.readInt()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'nullStringIntUnion': "+ unionIndex0));
                }
            }
        }
        return StringableSubRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord deserializeAnotherSubRecord0(Object reuse, Decoder decoder)
        throws IOException, URISyntaxException
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
