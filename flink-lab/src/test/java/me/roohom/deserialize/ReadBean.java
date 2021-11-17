package me.roohom.deserialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tendcloud.enterprise.analytics.collector.message.TDMessage;
import lombok.SneakyThrows;

import java.io.*;

public class ReadBean {
    private static final String FILE_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/2021-11-17--15/part-5-0";
    private static final String FILE_OUTPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/obj/data.bin";

    final static short STREAM_MAGIC = (short) 0xaced;
    final static short STREAM_VERSION = 5;

    @SneakyThrows
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();

        //FileInputStream fileInputStream = new FileInputStream(FILE_PATH);
        //FileOutputStream fileOutputStream = new FileOutputStream(FILE_OUTPUT_PATH);
        ////fileOutputStream.write(STREAM_MAGIC);
        ////fileOutputStream.write(STREAM_VERSION);
        //byte[] buf = new byte[2];
        //int len = -1;
        //while ((len = fileInputStream.read(buf)) != -1) {
        //    if (len != STREAM_MAGIC || len != STREAM_VERSION) {
        //        fileOutputStream.write(buf, 0, len);
        //    }
        //}
        //
        //fileInputStream.close();
        //fileOutputStream.close();


        TDMessage tdMessage = (TDMessage) new ObjectInputStream(new FileInputStream(FILE_OUTPUT_PATH)).readObject();
        Object eventPackage = new ObjectInputStream(new ByteArrayInputStream((byte[]) tdMessage.getData())).readObject();

        System.out.println(objectMapper.writeValueAsString(eventPackage));
    }
}
