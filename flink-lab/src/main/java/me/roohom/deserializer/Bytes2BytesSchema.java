package me.roohom.deserializer;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 读取Kafka中的二进制数据
 */
public class Bytes2BytesSchema implements KafkaDeserializationSchema<byte[]> {

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public byte[] deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        //do nothing here, just send raw message out.
        return record.value();
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}
