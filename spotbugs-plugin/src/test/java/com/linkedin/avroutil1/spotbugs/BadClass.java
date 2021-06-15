package com.linkedin.avroutil1.spotbugs;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;

public class BadClass {

    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = new BinaryEncoder(null);
    }

    public void instantiateBinaryDecoder() {
        BinaryDecoder bobTheDecoder = new BinaryDecoder(null);
    }
}
