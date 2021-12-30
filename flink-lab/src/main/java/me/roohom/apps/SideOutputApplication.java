package me.roohom.apps;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputApplication {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> integerStream = env.fromElements("1", "2-", "3x", "4", "5", "6-", "7v", "8", "9", "10");
        OutputTag<String> errorTag = new OutputTag<String>("wrongInteger") {
        };

        SingleOutputStreamOperator<Integer> processedStream = integerStream.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
                try {
                    int anInt = Integer.parseInt(value);
                    out.collect(anInt);
                } catch (Exception e) {
                    ctx.output(errorTag, value);
                }
            }
        });

        processedStream.map(
                x -> "正确数据 -> " + x
        ).print();
        processedStream.getSideOutput(errorTag).map(
                x -> "错误数据 -> " + x
        ).print();

        env.execute();
    }
}
