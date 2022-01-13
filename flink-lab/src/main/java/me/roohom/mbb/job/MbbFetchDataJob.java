package me.roohom.mbb.job;

import me.roohom.mbb.env.FlinkEnv;
import me.roohom.mbb.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MbbFetchDataJob {
    /**
     * 消费来自TSS的Kafka的数据
     *
     * @param properties 配置
     */
    public void handle(Properties properties) throws Exception {
        StreamExecutionEnvironment streamEnv = FlinkEnv.getStreamEnv();
        //SOURCE
        //SingleOutputStreamOperator<String> tssSourceStream = streamEnv.addSource(
        //        KafkaSource.source(
        //                properties.getProperty("kafka.senddata.topic"),
        //                properties.getProperty("kafka.sendata.bootstrap"),
        //                properties.getProperty("kafka.senddata.groupid")
        //        )
        //).name("fetchTssDataSource");

        DataStreamSource<String> tssSourceStream = streamEnv.readTextFile("data/mbb/received_data.json");

        //TRANSFORM

        //SINK
        tssSourceStream.print();

        streamEnv.execute("MBB data process job");
    }
}
