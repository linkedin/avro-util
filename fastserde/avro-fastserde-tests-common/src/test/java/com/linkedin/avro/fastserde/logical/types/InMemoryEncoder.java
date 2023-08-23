package com.linkedin.avro.fastserde.logical.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

public class InMemoryEncoder extends Encoder {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    private final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos);

    public BinaryDecoder toDecoder() {
        return DecoderFactory.get().binaryDecoder(toByteArray(), null);
    }

    public byte[] toByteArray() {
        try {
            binaryEncoder.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return baos.toByteArray();
    }

    // generated delegate methods

    @Override
    public void writeNull() throws IOException {
        binaryEncoder.writeNull();
    }

    @Override
    public void writeString(Utf8 utf8) throws IOException {
        binaryEncoder.writeString(utf8);
    }

    @Override
    public void writeString(String string) throws IOException {
        binaryEncoder.writeString(string);
    }

    @Override
    public void writeBytes(ByteBuffer bytes) throws IOException {
        binaryEncoder.writeBytes(bytes);
    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        binaryEncoder.writeBytes(bytes, start, len);
    }

    @Override
    public void writeEnum(int e) throws IOException {
        binaryEncoder.writeEnum(e);
    }

    @Override
    public void writeArrayStart() throws IOException {
        binaryEncoder.writeArrayStart();
    }

    @Override
    public void setItemCount(long itemCount) throws IOException {
        binaryEncoder.setItemCount(itemCount);
    }

    @Override
    public void startItem() throws IOException {
        binaryEncoder.startItem();
    }

    @Override
    public void writeArrayEnd() throws IOException {
        binaryEncoder.writeArrayEnd();
    }

    @Override
    public void writeMapStart() throws IOException {
        binaryEncoder.writeMapStart();
    }

    @Override
    public void writeMapEnd() throws IOException {
        binaryEncoder.writeMapEnd();
    }

    @Override
    public void writeIndex(int unionIndex) throws IOException {
        binaryEncoder.writeIndex(unionIndex);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        binaryEncoder.writeBoolean(b);
    }

    @Override
    public void writeInt(int n) throws IOException {
        binaryEncoder.writeInt(n);
    }

    @Override
    public void writeLong(long n) throws IOException {
        binaryEncoder.writeLong(n);
    }

    @Override
    public void writeFloat(float f) throws IOException {
        binaryEncoder.writeFloat(f);
    }

    @Override
    public void writeDouble(double d) throws IOException {
        binaryEncoder.writeDouble(d);
    }

    @Override
    public void writeString(CharSequence charSequence) throws IOException {
        binaryEncoder.writeString(charSequence);
    }

    @Override
    public void writeBytes(byte[] bytes) throws IOException {
        binaryEncoder.writeBytes(bytes);
    }

    @Override
    public void writeFixed(byte[] bytes, int start, int len) throws IOException {
        binaryEncoder.writeFixed(bytes, start, len);
    }

    @Override
    public void writeFixed(byte[] bytes) throws IOException {
        binaryEncoder.writeFixed(bytes);
    }

    @Override
    public void writeFixed(ByteBuffer bytes) throws IOException {
        binaryEncoder.writeFixed(bytes);
    }

    @Override
    public void flush() throws IOException {
        binaryEncoder.flush();
    }
}
