package me.roohom.encoder;

import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;

public class SimpleBytesEncoderBytes implements Encoder<byte[]> {
    @Override
    public void encode(byte[] bytes, OutputStream outputStream) throws IOException {
        outputStream.write(bytes, 4, bytes.length);
    }
}
