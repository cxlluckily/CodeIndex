package me.roohom.mbb.sink;

import me.roohom.mbb.serializer.AvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Properties;

public class KafkaPro<T extends SpecificRecordBase> implements Serializable {
    /**
     * 获得Kafka配置
     *
     * @return Kafka配置信息
     */
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.122.44.113:9092,10.122.44.114:9092,10.122.44.115:9092");
        //properties.put("bootstrap.servers", "101.35.21.127:9092");
        properties.put("acks", "1");
        properties.put("retries", "0");
        properties.put("batch.size", "16384");
        properties.put("linger.ms", "1");
        properties.put("buffer.memory", "33554421");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", AvroSerializer.class.getName());

        return properties;
    }

    /**
     * 获取kafka生产者对象
     */
    KafkaProducer<String, T> producer = new KafkaProducer<>(getProperties());


    public void sendData(String topic, T data) {
        producer.send(new ProducerRecord(topic, data));
    }
}
