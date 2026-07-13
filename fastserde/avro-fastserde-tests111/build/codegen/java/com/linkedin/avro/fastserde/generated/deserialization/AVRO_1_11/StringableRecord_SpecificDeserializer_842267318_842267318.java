
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
import com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableSubRecord;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class StringableRecord_SpecificDeserializer_842267318_842267318
    implements FastDeserializer<StringableRecord>
{

    private final Schema readerSchema;

    public StringableRecord_SpecificDeserializer_842267318_842267318(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public StringableRecord deserialize(StringableRecord reuse, Decoder decoder, DatumReaderCustomization customization)
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

    public StringableRecord deserializeStringableRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        StringableRecord stringableRecord0;
        if ((reuse)!= null) {
            stringableRecord0 = ((StringableRecord)(reuse));
        } else {
            stringableRecord0 = new StringableRecord();
        }
        BigInteger charSequence0 = new BigInteger((decoder.readString()));
        stringableRecord0 .put(0, charSequence0);
        populate_StringableRecord0((stringableRecord0), (customization), (decoder));
        populate_StringableRecord1((stringableRecord0), (customization), (decoder));
        populate_StringableRecord2((stringableRecord0), (customization), (decoder));
        populate_StringableRecord3((stringableRecord0), (customization), (decoder));
        populate_StringableRecord4((stringableRecord0), (customization), (decoder));
        return stringableRecord0;
    }

    private void populate_StringableRecord0(StringableRecord stringableRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, URISyntaxException
    {
        BigDecimal charSequence1 = new BigDecimal((decoder.readString()));
        stringableRecord0 .put(1, charSequence1);
        URI charSequence2 = new URI((decoder.readString()));
        stringableRecord0 .put(2, charSequence2);
    }

    private void populate_StringableRecord1(StringableRecord stringableRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        URL charSequence3 = new URL((decoder.readString()));
        stringableRecord0 .put(3, charSequence3);
        File charSequence4 = new File((decoder.readString()));
        stringableRecord0 .put(4, charSequence4);
    }

    private void populate_StringableRecord2(StringableRecord stringableRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        List<URL> urlArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = stringableRecord0 .get(5);
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
        stringableRecord0 .put(5, urlArray0);
        Map<URL, BigInteger> urlMap0 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            urlMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(stringableRecord0 .get(6), ((int) chunkLen1)));
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    URL key0 = new URL((decoder.readString()));
                    BigInteger charSequence6 = new BigInteger((decoder.readString()));
                    urlMap0 .put(key0, charSequence6);
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            urlMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(stringableRecord0 .get(6), 0));
        }
        stringableRecord0 .put(6, urlMap0);
    }

    private void populate_StringableRecord3(StringableRecord stringableRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        stringableRecord0 .put(7, deserializeStringableSubRecord0(stringableRecord0 .get(7), (decoder), (customization)));
        stringableRecord0 .put(8, deserializeAnotherSubRecord0(stringableRecord0 .get(8), (decoder), (customization)));
    }

    public StringableSubRecord deserializeStringableSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, URISyntaxException
    {
        StringableSubRecord stringableSubRecord0;
        if ((reuse)!= null) {
            stringableSubRecord0 = ((StringableSubRecord)(reuse));
        } else {
            stringableSubRecord0 = new StringableSubRecord();
        }
        URI charSequence7 = new URI((decoder.readString()));
        stringableSubRecord0 .put(0, charSequence7);
        populate_StringableSubRecord0((stringableSubRecord0), (customization), (decoder));
        return stringableSubRecord0;
    }

    private void populate_StringableSubRecord0(StringableSubRecord stringableSubRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, URISyntaxException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            stringableSubRecord0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence8;
                Object oldString0 = stringableSubRecord0 .get(1);
                if (oldString0 instanceof Utf8) {
                    charSequence8 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence8 = (decoder).readString(null);
                }
                stringableSubRecord0 .put(1, charSequence8);
            } else {
                if (unionIndex0 == 2) {
                    stringableSubRecord0 .put(1, (decoder.readInt()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'nullStringIntUnion': "+ unionIndex0));
                }
            }
        }
    }

    public AnotherSubRecord deserializeAnotherSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException, URISyntaxException
    {
        AnotherSubRecord anotherSubRecord0;
        if ((reuse)!= null) {
            anotherSubRecord0 = ((AnotherSubRecord)(reuse));
        } else {
            anotherSubRecord0 = new AnotherSubRecord();
        }
        anotherSubRecord0 .put(0, deserializeStringableSubRecord0(anotherSubRecord0 .get(0), (decoder), (customization)));
        return anotherSubRecord0;
    }

    private void populate_StringableRecord4(StringableRecord stringableRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException, NumberFormatException, MalformedURLException, URISyntaxException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            stringableRecord0 .put(9, null);
        } else {
            if (unionIndex1 == 1) {
                String charSequence9 = (decoder).readString();
                stringableRecord0 .put(9, charSequence9);
            } else {
                throw new RuntimeException(("Illegal union index for 'stringUnion': "+ unionIndex1));
            }
        }
    }

}
