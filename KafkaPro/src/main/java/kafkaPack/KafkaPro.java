package kafkaPack;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @ClassName: KafkaProducer
 * @Author: Roohom
 * @Function: Kafka生产者，往kafka生产数据
 * @Date: 2020/10/28 17:14
 * @Software: IntelliJ IDEA
 */
public class KafkaPro {

    /**
     * 获得Kafka配置
     *
     * @return Kafka配置信息
     */
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.put("acks", "0");
        properties.put("retries", "0");
        properties.put("batch.size", "16384");
        properties.put("linger.ms", "1");
        properties.put("buffer.memory", "33554421");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        return properties;
    }

    /**
     * 获取kafka生产者对象
     */
    KafkaProducer producer = new KafkaProducer<>(getProperties());


    public void sendData(String topic, String data) {
        producer.send(new ProducerRecord(topic, data));

    }


}
