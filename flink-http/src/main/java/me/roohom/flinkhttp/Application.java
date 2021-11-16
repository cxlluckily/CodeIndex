package me.roohom.flinkhttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.SneakyThrows;
import me.roohom.flinkhttp.source.HttpPostSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Application {
    private static final String URL = "http://101.35.21.127:8802";

    @SneakyThrows
    public static void main(String[] args) throws JsonProcessingException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<String> streamSource = env.readTextFile("flink-http/mock/data.json");
        DataStreamSource<String> streamSource = env.addSource(new HttpPostSource(URL, 1000, "", new SimpleStringSchema()));

        ////////////////// TEST HTTP SINK FOLLOWING \\\\\\\\\\\\\\\\\
        //设置endpoint
        //String endpoint = "http://101.35.21.127:8801/add/student/";
        //String endpoint = "http://localhost:8080/api/postdata/";
        //
        ////设置header
        //HashMap<String, String> headerMap = new HashMap<>();
        //headerMap.put("Content-Type", "application/json");
        //
        //HTTPConnectionConfig httpConnectionConfig = new HTTPConnectionConfig(
        //        endpoint,
        //        HTTPConnectionConfig.HTTPMethod.POST,
        //        headerMap,
        //        false
        //);
        ////////////////// TEST HTTP SINK ABOVE \\\\\\\\\\\\\\\\\

        streamSource
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                })
                .print()
        //HTTP SINK
        //.addSink(new HTTPSink<>(httpConnectionConfig))
        ;

        env.execute();


    }
}
