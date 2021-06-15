package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;

import java.io.InputStream;
import java.io.OutputStream;

public class GoodClass {

    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = AvroCompatibilityHelper.newBinaryEncoder( (OutputStream) null);
    }

    public void instantiateBinaryDecoder() {
        BinaryDecoder bobTheDecoder = AvroCompatibilityHelper.newBinaryDecoder( (InputStream) null);
        BinaryDecoder robertTheDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[] {1, 2, 3}, null);
    }
}
