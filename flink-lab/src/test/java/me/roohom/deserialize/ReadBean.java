package me.roohom.deserialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tendcloud.enterprise.analytics.collector.message.TDMessage;
import lombok.SneakyThrows;

import java.io.*;

public class ReadBean {
    private static final String FILE_PATH = "flink-lab/data/2021-11-17--15/part-5-1";
    private static final String FILE_OUTPUT_PATH = "flink-lab/data/obj";

    final static short STREAM_MAGIC = (short) 0xaced;
    final static short STREAM_VERSION = 5;

    @SneakyThrows
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();

        FileInputStream fileInputStream = new FileInputStream(FILE_PATH);
        FileOutputStream fileOutputStream = new FileOutputStream(FILE_OUTPUT_PATH);
        //fileOutputStream.write(STREAM_MAGIC);
        //fileOutputStream.write(STREAM_VERSION);
        byte[] buf = new byte[2];
        int len = -1;
        int step = 0;
        byte[] lastByte = new byte[2];
        while ((len = fileInputStream.read(buf)) != -1) {
            if (step <= 1) {
                //do nothing.
                lastByte = buf;
            }
            if (len != STREAM_MAGIC || len != STREAM_VERSION) {
                fileOutputStream.write(buf, 0, len);
            }
            step++;
        }

        fileInputStream.close();
        fileOutputStream.close();


//        TDMessage tdMessage = (TDMessage) new ObjectInputStream(new FileInputStream(FILE_PATH)).readObject();
//        Object eventPackage = new ObjectInputStream(new ByteArrayInputStream((byte[]) tdMessage.getData())).readObject();

//        System.out.println(objectMapper.writeValueAsString(eventPackage));
    }
}
