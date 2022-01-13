package com.svw.bdp.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSink {

    public static FlinkKafkaProducer<String> sink(String topic, String bootStrapServers, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServers);
        properties.setProperty("group.id", groupId);

        return new FlinkKafkaProducer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }
}
