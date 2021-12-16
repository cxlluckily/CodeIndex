package me.roohom.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class HiveQueryHandleApplication {

    private static final String catalogName = "default";
    private static final String defaultDatabase = "test";
//    private static final String hiveConfDir = "/etc/hive/Config";
    private static final String hiveConfDir = "D:\\export\\Config";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = getStreamTableEnv(env);

        Table omd3 = streamTableEnv.sqlQuery("SELECT * FROM test.omd3");

        DataStream<Row> rowDataStream = streamTableEnv.toAppendStream(omd3, Row.class);
        rowDataStream.print();

        env.execute();
    }

    /**
     * 获取StreamTableEnvironment
     *
     * @param env 流处理执行环境
     * @return a 获取StreamTableEnvironment
     */
    public static StreamTableEnvironment getStreamTableEnv(StreamExecutionEnvironment env) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        streamTableEnv.registerCatalog(catalogName, hiveCatalog);
        streamTableEnv.useCatalog(catalogName);
        streamTableEnv.useDatabase(defaultDatabase);

        return streamTableEnv;
    }

}
