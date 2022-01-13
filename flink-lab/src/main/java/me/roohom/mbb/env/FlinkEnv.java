package me.roohom.mbb.env;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkEnv {
    /**
     * 获取Flink Stream ENV
     *
     * @return a StreamExecutionEnvironment, yeah, you know
     */
    public static StreamExecutionEnvironment getStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //CHECKPOINT
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //RESTART STRATEGY
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                Time.of(10, TimeUnit.SECONDS)));

        return env;
    }

    /**
     * 获取Flink StreamTableEnvironment
     * @param properties 配置
     * @return a StreamTableEnvironment, yeah, you also know
     */
    public static StreamTableEnvironment getStreamTableEnv(StreamExecutionEnvironment env, Properties properties) {
        EnvironmentSettings batchSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, batchSetting);

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
