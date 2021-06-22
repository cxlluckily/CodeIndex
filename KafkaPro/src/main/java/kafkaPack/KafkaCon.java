package kafkaPack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaCon {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.122.44.115:9092,10.122.44.114:9092,10.122.44.113:9092");

        props.put("group.id", Integer.valueOf(new Random().nextInt()).toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅的topic, 可同时订阅多个
        ArrayList<String> topics = new ArrayList<>();
        topics.add("cdp.mos-pms-mysql-spm");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> records = consumer.poll(1000L);
        for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            System.out.println(record.value());

    }
}

