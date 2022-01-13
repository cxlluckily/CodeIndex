package me.roohom.mbb.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * 仅需要复写此方法即可以实现自定义序列化
     *
     * @param topic Kafka topic
     * @param data  数据
     * @return 输出流字节数组
     */
    @Override
    public byte[] serialize(String topic, T data) {
        //设置Schema约束对象
        SpecificDatumWriter<Object> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            //通过write方法，会将avro类型的数据，编码成字节流，数据会存储在outputStream字节输出流对象中
            datumWriter.write(data, binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputStream.toByteArray();
    }

    @Override
    public void close() {

    }
}