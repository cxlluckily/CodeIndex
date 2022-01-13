package me.roohom.mbb.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.roohom.mbb.env.FlinkEnv;
import me.roohom.mbb.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Properties;

public class BatchSubscribeJob {

    /**
     * 从Hive表中拿去订阅数据发送给TSS的Kafka
     *
     * @param properties 配置
     * @throws Exception
     */
    public void handle(Properties properties) throws Exception {

        StreamExecutionEnvironment env = FlinkEnv.getStreamEnv();
        StreamTableEnvironment streamTableEnv = FlinkEnv.getStreamTableEnv(env, properties);
        streamTableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //获取订阅数据 Row(vin, correlationId, dataIdList)
        //Table table = streamTableEnv.sqlQuery("SELECT * FROM " + properties.getProperty("hive.subscribe"));
        Table table = streamTableEnv.sqlQuery("SELECT 'BAUZZZTEST1234567' AS vin, '31415926525abc' AS correlationId, '0x0101020001,0x0101020002,0x020203FFFF' AS dataIdList");
        DataStream<Row> subscribeStream = streamTableEnv.toAppendStream(table, Row.class);
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();

        ObjectMapper objectMapper = new ObjectMapper();

        SingleOutputStreamOperator<String> row2JsonStream = subscribeStream.map(
                x ->
                {
                    HashMap<String, Object> arity = new HashMap<>();
                    for (int i = 0; i < x.getArity(); i++) {
                        arity.put(fieldNames[i], x.getField(i));
                    }
                    return objectMapper.writeValueAsString(arity);
                }
        ).name("subscribe2JsonMap");

        //SINK
        row2JsonStream.addSink(
                KafkaSink.sink(
                        properties.getProperty("kafka.producer.subscribe.topic"),
                        properties.getProperty("kafka.producer.bootstrap"),
                        properties.getProperty("kafka.producer.groupid")
                )
        ).name("tssKafkaSink");

        env.execute("MBB subscribe job");
    }
}
