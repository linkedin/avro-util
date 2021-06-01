package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.io.BinaryEncoder;

import java.io.OutputStream;

public class GoodClass {

    public void instantiateBinaryEncoder() {
        BinaryEncoder bobTheEncoder = AvroCompatibilityHelper.newBinaryEncoder( (OutputStream) null);
    }
}
