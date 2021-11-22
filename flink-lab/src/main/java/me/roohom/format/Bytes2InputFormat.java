package me.roohom.format;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

public class Bytes2InputFormat extends FileInputFormat<byte[]> {

    private Path path;

    public Bytes2InputFormat(Path path) {
        this.path = path;
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        FileInputSplit fileInputSplit = new FileInputSplit(1, path, 0L, -1, null);
        super.open(fileInputSplit);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public byte[] nextRecord(byte[] reuse) throws IOException {
        return new byte[0];
    }

}