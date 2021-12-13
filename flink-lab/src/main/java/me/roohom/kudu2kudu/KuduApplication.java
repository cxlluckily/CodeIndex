package me.roohom.kudu2kudu;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.colloh.flink.kudu.connector.table.catalog.KuduCatalog;

public class KuduApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(6000L);
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, streamSettings);

        ////方式一
//        streamTableEnv.registerCatalog("kudu", new KuduCatalog("cdh001:7051"));
//        streamTableEnv.useCatalog("kudu");
//
//        Table table = streamTableEnv.sqlQuery("SELECT id,name FROM upsert_kudu_test");
//        DataStream<Row> rows = streamTableEnv.toAppendStream(table, Row.class);
//        rows.print();

        /////方式二
        streamTableEnv.executeSql("CREATE TABLE IF NOT EXISTS test (\n" +
                " id bigint,\n" +
                " name string\n" +
                ") WITH (\n" +
                "  'connector' = 'kudu',\n" +
                "  'kudu.masters' = 'cdh001:7051',\n" +
                "  'kudu.table' = 'impala::dw_unicdata.test',\n" +
                "  'kudu.hash-columns' = 'id',\n" +
                "  'kudu.primary-key-columns' = 'id'\n" +
                ")");
        Table table = streamTableEnv.sqlQuery("select * from test");
        DataStream<Row> rowStream = streamTableEnv.toAppendStream(table, Row.class);
        rowStream.print();
//        streamTableEnv.executeSql("upsert into test select 3 as id, 'mmm' as  name").print();

        streamTableEnv.executeSql("CREATE TABLE IF NOT EXISTS test2 (\n" +
                " id bigint,\n" +
                " name string\n" +
                ") WITH (\n" +
                "  'connector' = 'kudu',\n" +
                "  'kudu.masters' = 'cdh001:7051',\n" +
                "  'kudu.table' = 'test2',\n" +
                "  'kudu.hash-columns' = 'id',\n" +
                "  'kudu.primary-key-columns' = 'id'\n" +
                ")");
        //从test表抽取数据插入test2表
        streamTableEnv.executeSql("upsert into test2 select * from test").print();


        env.execute("Flink read kudu streaming");
    }
}
