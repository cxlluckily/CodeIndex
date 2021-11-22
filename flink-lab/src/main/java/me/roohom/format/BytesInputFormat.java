package me.roohom.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tendcloud.enterprise.analytics.collector.message.TDMessage;
import lombok.SneakyThrows;
import me.roohom.bean.Person;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class BytesInputFormat extends DelimitedInputFormat<ArrayList<String>> {
    private final Logger LOG = LoggerFactory.getLogger(BytesInputFormat.class);

    private static final byte NEW_LINE = (byte) '\n';

    final static short STREAM_MAGIC = (short) 0xaced;

    final static short STREAM_VERSION = 5;

    private static byte[] NEW_OBJECT = null;

    //private byte[] delimiter = new byte[] {NEW_OBJECT};

    private String charsetName = "UTF-8";

    public static byte[] shortToBytes(short[] shorts) {
        if (shorts == null) {
            return null;
        }
        byte[] bytes = new byte[shorts.length * 2];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(shorts);

        return bytes;
    }

    public byte[] initial(short[] something) {
        NEW_OBJECT = shortToBytes(something);
        return NEW_OBJECT;
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        short[] shorts = {STREAM_MAGIC, STREAM_VERSION};
        initial(shorts);
    }

    @SneakyThrows
    @Override
    public ArrayList<String> readRecord(ArrayList<String> reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<String> POJOJsonList = new ArrayList<>();
        byte[] bs = new byte[4];
        System.arraycopy(bytes, offset + numBytes - 4, bs, 0, 4);
        if (offset + numBytes >= 1
                && bs == NEW_OBJECT) {
            numBytes -= 4;
        }

        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes, offset, numBytes);
            //ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes, offset, numBytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            while (true) {
                Person person = (Person) objectInputStream.readObject();
                POJOJsonList.add(objectMapper.writeValueAsString(person));
                //TDMessage tdMessage = (TDMessage) objectInputStream.readObject();
                //Object eventPackage = new ObjectInputStream(new ByteArrayInputStream((byte[]) tdMessage.getData())).readObject();
                //POJOJsonList.add(objectMapper.writeValueAsString(eventPackage));
            }
        } catch (Exception E) {
            LOG.info("We reached the end of those bytes.");
        }

        return POJOJsonList;
    }

    /**
     * 设置delimiter
     *
     * @param delimiter 分隔符
     */
    @Override
    public void setDelimiter(byte[] delimiter) {
        super.setDelimiter(NEW_OBJECT);
    }

    @Override
    public void setBufferSize(int bufferSize) {
        super.setBufferSize(100);
    }
}
