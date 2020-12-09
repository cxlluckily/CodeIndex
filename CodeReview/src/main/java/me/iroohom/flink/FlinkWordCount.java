package me.iroohom.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: FlinkWordCount
 * @Author: Roohom
 * @Function: Flink基础案例之词频统计
 * @Date: 2020/12/2 20:29
 * @Software: IntelliJ IDEA
 */
public class FlinkWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> inputSocketStream = env.socketTextStream("node1", 9999);

        /**
         * 词频统计
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputSocketStream
                //过滤数据
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value != null && value.trim().length() > 0;
                    }
                })
                //流中数据扁平化
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.trim().split("\\s+");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }).setParallelism(3)
                // word -> (word,1)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(0)
                .sum(1);


        resultStream.printToErr();


        env.execute(FlinkWordCount.class.getSimpleName());


    }
}
