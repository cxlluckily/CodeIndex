package me.iroohom.main;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * @ClassName: StreamWordCount
 * @Author: Roohom
 * @Function: 流处理wordcount
 * @Software: IntelliJ IDEA
 */
public class StreamWordCount {

    public static final Logger logger = LoggerFactory.getLogger(StreamWordCount.class);

    public static void main(String[] args) throws Exception {

        logger.info("//////////////Streaming word count start here////////////");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("node1", 8090);
        logger.warn("/////////////////////SOURCE LOADED HERE///////////////");
        logger.debug("//////////////////THIS IS A DEBUG SENTENCE//////////////////////");
        logger.error("//////////////////THIS IS A ERROR SENTENCE//////////////////////");
        source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(0)
                .sum(1)
                .print(); //数据打印(流处理中不是触发算子)
        logger.info("//////////////////Calculation Start Here////////////////");
        //需要触发执行
        env.execute();
        logger.info("//////////////Streaming word count stop here////////////");
    }
}
