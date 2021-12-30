package me.roohom.apps;

import me.roohom.source.HttpSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HttpApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> httpSource = env.addSource(new HttpSource("", 1000L, new SimpleStringSchema()));
        httpSource.print();


        env.execute("Http source on flink");
    }
}
