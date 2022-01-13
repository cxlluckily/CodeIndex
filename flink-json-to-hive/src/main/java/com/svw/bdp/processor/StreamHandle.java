package com.svw.bdp.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.svw.bdp.function.SchemaMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.*;

public class StreamHandle {
    public void doIt(Properties properties) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new MemoryStateBackend());

        //StreamTableEnvironment streamTableEnv = getStreamTableEnv(env, properties);

        ObjectMapper objectMapper = new ObjectMapper();
        //DataStreamSource<String> streamSource = env.addSource(
        //        KafkaSource.source(
        //                properties.getProperty("kafka.source.topic"),
        //                properties.getProperty("kafka.source.bootstrap"),
        //                properties.getProperty("kafka.source.groupid")
        //        )
        //);

        DataStreamSource<String> streamSource = env.readTextFile("mock/mock.json").setParallelism(1);

        //MapStateDescriptor<String, String> schemaBroadcastStream = new MapStateDescriptor<>(
        //        "schemaBroadcast",
        //        BasicTypeInfo.STRING_TYPE_INFO,
        //        BasicTypeInfo.STRING_TYPE_INFO
        //);
        //
        //SingleOutputStreamOperator<HashMap<String, String>> schemaStream = env.readTextFile("mock/mock.json").setParallelism(1)
        //        .map(
        //        new MapFunction<String, HashMap<String, String>>() {
        //            @Override
        //            public HashMap<String, String> map(String value) throws Exception {
        //                HashMap<String, String> schema = new HashMap<>();
        //                JsonNode jsonNode = objectMapper.readTree(value);
        //                Iterator<String> stringIterator = jsonNode.fieldNames();
        //                while (stringIterator.hasNext()) {
        //                    String next = stringIterator.next();
        //                    schema.put(next, "String");
        //                }
        //                return schema;
        //            }
        //        }
        //).setParallelism(1);
        //
        //schemaStream.print().setParallelism(1);

        streamSource.setParallelism(1)
                .keyBy(x -> objectMapper.readValue(x, Map.class).get("id").toString())
                .map(new SchemaMapFunction(objectMapper)).print().setParallelism(1);

        //streamSource.print().setParallelism(1);

        env.execute();
    }


    /**
     * 获取流表执行环境
     *
     * @param env        流环境
     * @param properties flink sink hive的配置
     * @return 流表执行环境
     */
    public StreamTableEnvironment getStreamTableEnv(StreamExecutionEnvironment env, Properties properties) {
        EnvironmentSettings streamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, streamSetting);
        HiveCatalog hive = new HiveCatalog(
                properties.getProperty("hive.catalog"),
                properties.getProperty("hive.database"),
                properties.getProperty("hive.conf.dir")
        );
        streamTableEnv.registerCatalog(properties.getProperty("hive.catalog"), hive);
        streamTableEnv.useCatalog(properties.getProperty("hive.catalog"));
        streamTableEnv.useDatabase(properties.getProperty("hive.database"));

        return streamTableEnv;
    }
}
