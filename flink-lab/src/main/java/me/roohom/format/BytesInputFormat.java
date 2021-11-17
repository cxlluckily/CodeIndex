package me.roohom.format;

import org.apache.flink.api.common.io.FileInputFormat;

import java.io.IOException;

public class BytesInputFormat extends FileInputFormat<String> {

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public String nextRecord(String reuse) throws IOException {
        return reuse;
    }
}
