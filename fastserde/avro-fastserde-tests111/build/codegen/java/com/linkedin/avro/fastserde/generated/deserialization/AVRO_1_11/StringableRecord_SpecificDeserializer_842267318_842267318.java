
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class StringableRecord_SpecificDeserializer_842267318_842267318
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.StringableRecord>
{

    private final Schema readerSchema;

    public StringableRecord_SpecificDeserializer_842267318_842267318(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserialize(com.linkedin.avro.fastserde.generated.avro.StringableRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        try {
            return deserializeStringableRecord0((reuse), (decoder), (customization));
        } catch (NumberFormatException e) {
            throw new AvroRuntimeException(e);
        } catch (MalformedURLException e) {
            throw new AvroRuntimeException(e);
        } catch (URISyntaxException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableRecord deserializeStringableRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord;
        if ((reuse)!= null) {
            StringableRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableRecord)(reuse));
        } else {
            StringableRecord = new com.linkedin.avro.fastserde.generated.avro.StringableRecord();
        }
        BigInteger charSequence0 = new BigInteger((decoder.readString()));
        StringableRecord.put(0, charSequence0);
        populate_StringableRecord0((StringableRecord), (customization), (decoder));
        populate_StringableRecord1((StringableRecord), (customization), (decoder));
        populate_StringableRecord2((StringableRecord), (customization), (decoder));
        populate_StringableRecord3((StringableRecord), (customization), (decoder));
        populate_StringableRecord4((StringableRecord), (customization), (decoder));
        return StringableRecord;
    }

    private void populate_StringableRecord0(com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, URISyntaxException
    {
        BigDecimal charSequence1 = new BigDecimal((decoder.readString()));
        StringableRecord.put(1, charSequence1);
        URI charSequence2 = new URI((decoder.readString()));
        StringableRecord.put(2, charSequence2);
    }

    private void populate_StringableRecord1(com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        URL charSequence3 = new URL((decoder.readString()));
        StringableRecord.put(3, charSequence3);
        File charSequence4 = new File((decoder.readString()));
        StringableRecord.put(4, charSequence4);
    }

    private void populate_StringableRecord2(com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        List<URL> urlArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = StringableRecord.get(5);
        if (oldArray0 instanceof List) {
            urlArray0 = ((List) oldArray0);
            if (urlArray0 instanceof GenericArray) {
                ((GenericArray) urlArray0).reset();
            } else {
                urlArray0 .clear();
            }
        } else {
            urlArray0 = new ArrayList<URL>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object urlArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    urlArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                URL charSequence5 = new URL((decoder.readString()));
                urlArray0 .add(charSequence5);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        StringableRecord.put(5, urlArray0);
        Map<URL, BigInteger> urlMap0 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            urlMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(StringableRecord.get(6), ((int) chunkLen1)));
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    URL key0 = new URL((decoder.readString()));
                    BigInteger charSequence6 = new BigInteger((decoder.readString()));
                    urlMap0 .put(key0, charSequence6);
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            urlMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(StringableRecord.get(6), 0));
        }
        StringableRecord.put(6, urlMap0);
    }

    private void populate_StringableRecord3(com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        StringableRecord.put(7, deserializeStringableSubRecord0(StringableRecord.get(7), (decoder), (customization)));
        StringableRecord.put(8, deserializeAnotherSubRecord0(StringableRecord.get(8), (decoder), (customization)));
    }

    public com.linkedin.avro.fastserde.generated.avro.StringableSubRecord deserializeStringableSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, URISyntaxException
    {
        com.linkedin.avro.fastserde.generated.avro.StringableSubRecord StringableSubRecord;
        if ((reuse)!= null) {
            StringableSubRecord = ((com.linkedin.avro.fastserde.generated.avro.StringableSubRecord)(reuse));
        } else {
            StringableSubRecord = new com.linkedin.avro.fastserde.generated.avro.StringableSubRecord();
        }
        URI charSequence7 = new URI((decoder.readString()));
        StringableSubRecord.put(0, charSequence7);
        populate_StringableSubRecord0((StringableSubRecord), (customization), (decoder));
        return StringableSubRecord;
    }

    private void populate_StringableSubRecord0(com.linkedin.avro.fastserde.generated.avro.StringableSubRecord StringableSubRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, URISyntaxException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            StringableSubRecord.put(1, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence8;
                Object oldString0 = StringableSubRecord.get(1);
                if (oldString0 instanceof Utf8) {
                    charSequence8 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence8 = (decoder).readString(null);
                }
                StringableSubRecord.put(1, charSequence8);
            } else {
                if (unionIndex0 == 2) {
                    StringableSubRecord.put(1, (decoder.readInt()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'nullStringIntUnion': "+ unionIndex0));
                }
            }
        }
    }

    public com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord deserializeAnotherSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, URISyntaxException
    {
        com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord AnotherSubRecord;
        if ((reuse)!= null) {
            AnotherSubRecord = ((com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord)(reuse));
        } else {
            AnotherSubRecord = new com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord();
        }
        AnotherSubRecord.put(0, deserializeStringableSubRecord0(AnotherSubRecord.get(0), (decoder), (customization)));
        return AnotherSubRecord;
    }

    private void populate_StringableRecord4(com.linkedin.avro.fastserde.generated.avro.StringableRecord StringableRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            StringableRecord.put(9, null);
        } else {
            if (unionIndex1 == 1) {
                String charSequence9 = (decoder).readString();
                StringableRecord.put(9, charSequence9);
            } else {
                throw new RuntimeException(("Illegal union index for 'stringUnion': "+ unionIndex1));
            }
        }
    }

}
