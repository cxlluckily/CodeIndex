package me.roohom.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.io.FileInputStream;

public class BinarySource extends RichSourceFunction<byte[]> {
    private volatile Boolean isRunning = true;
    private String filePath;

    public BinarySource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {

        File file = new File(filePath);
        FileInputStream fi = new FileInputStream(file);
        byte[] buffer = new byte[413264];
        fi.read(new byte[10]);
        int count = 0;
        while (isRunning) {
            int read = fi.read(buffer, 0, 413264);
            if (read == 413264) {
                ctx.collect(buffer);
            } else {
                isRunning = false;
            }
        }
        fi.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
