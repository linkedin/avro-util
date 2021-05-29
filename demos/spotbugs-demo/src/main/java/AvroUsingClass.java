import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AvroUsingClass {

    public static void main(String[] args) {
        BinaryDecoder decoder = new BinaryDecoder(new ByteArrayInputStream(new byte[] {1, 2, 3}));
        BinaryEncoder encoder = new BinaryEncoder(new ByteArrayOutputStream());
    }
}
