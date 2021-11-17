package me.roohom.encoder;

import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class SimpleBytesEncoder<IN> implements Encoder<IN> {
    private static final long serialVersionUID = 1L;

    private String charsetName;

    private transient Charset charset;

    public SimpleBytesEncoder() {
        this("UTF-8");
    }

    public SimpleBytesEncoder(String charsetName) {
        this.charsetName = charsetName;
    }

    @Override
    public void encode(IN element, OutputStream stream) throws IOException {
        if (charset == null) {
            charset = Charset.forName(charsetName);
        }

        stream.write((byte[]) element);

    }
}
