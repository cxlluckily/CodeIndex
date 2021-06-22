package kafkaPack;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class TestKafka {
    public static void main(String[] args) {
        Properties properties = KafkaUtil.getDefaultConsumerStringProperties("10.122.44.113:9092,10.122.44.114:9092,10.122.44.115:9092");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("cdp.mos-pms-mysql-spm"));
        ConsumerRecords<String, String> records = consumer.poll(1000L);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.value());
        }
    }


}
