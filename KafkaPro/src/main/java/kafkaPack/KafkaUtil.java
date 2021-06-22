package kafkaPack;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class KafkaUtil {

    public static Properties getDefaultProducerStringProperties(String KafkaBrokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaBrokers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public static void produceDataToKafka(String topic, String key, String value, KafkaProducer kafkaProducer) {
        Future future = kafkaProducer.send(new ProducerRecord(topic, key, value));
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static Properties getDefaultConsumerStringProperties(String KafkaBrokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaBrokers);
        Integer i = new Random().nextInt(1000);
        properties.put("group.id", i.toString());
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static ArrayList<String> getDefaultKafkaConsumer(String kafkaBroker, String topic) {
        Properties properties = getDefaultConsumerStringProperties(kafkaBroker);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        ArrayList<String> msg = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            msg.add(record.value());
        }
        return msg;
    }
}
