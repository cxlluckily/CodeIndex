package me.roohom.mbb.job;

import me.roohom.mbb.env.FlinkEnv;
import me.roohom.mbb.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class BatchFeedbackJob {
    /**
     * 接收TSS的反馈并将数据写入到一个Hive表中
     *
     * @param properties 配置
     * @throws Exception
     */
    public void handle(Properties properties) throws Exception {
        StreamExecutionEnvironment env = FlinkEnv.getStreamEnv();
        StreamTableEnvironment streamTableEnv = FlinkEnv.getStreamTableEnv(env, properties);

        streamTableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        SingleOutputStreamOperator<String> feedbackStream = env.addSource(
                KafkaSource.source(
                        properties.getProperty("kafka.feedback.topic"),
                        properties.getProperty("kafka.feedback.bootstrap"),
                        properties.getProperty("kafka.feedback.groupid")
                )
        ).name("feedbackKafka");

        streamTableEnv.createTemporaryView("feedback", feedbackStream);
        streamTableEnv.executeSql("INSERT INTO TABLE " + properties.getProperty(""));

        env.execute("MBB feedback to hive job");
    }
}
