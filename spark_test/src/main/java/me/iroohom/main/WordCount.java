package me.iroohom.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: WordCount
 * @Author: Roohom
 * @Function: wordcount
 * @Date: 2020/10/20 21:24
 * @Software: IntelliJ IDEA
 */
public class WordCount {

    public static final Logger logger = LoggerFactory.getLogger(WordCount.class);


    public static void main(String[] args) throws Exception {
        /**
         * 单词统计
         * 1.初始化环境
         * 2.加载数据源
         * 3.数据转换
         * 4.数据打印
         * 5.触发执行
         */

        //初始化环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //加载数据源
        DataSource<String> source1 = env.fromElements("a", "b", "c", "d");
        DataSource<String> source2 = env.fromElements("a b c d dd ss a b");
        logger.info("//////////数据源初始化成功///////");
        logger.debug("正在进行中...");
        source2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split(" ");
                for (String s1 : arr) {
                    out.collect(s1);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).groupBy(0)
                .sum(1)
                .print();
        logger.warn("WARN测试...");
    }
}
