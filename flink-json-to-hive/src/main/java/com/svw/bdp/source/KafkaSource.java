package com.svw.bdp.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {
    public static FlinkKafkaConsumer<String> source(String topic, String bootStrapServers, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServers);
        properties.setProperty("group.id", groupId);
        return new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }
}
