package me.iroohom.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: FlinkWordCount
 * @Author: Roohom
 * @Function: Flink基础案例之词频统计
 * @Date: 2020/12/2 20:29
 * @Software: IntelliJ IDEA
 */
public class FlinkWordCountProcess {
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
                })
                // word -> (word,1)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private transient ValueState<Integer> countValueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("countValueState", Integer.class);
                        countValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                        //获取上一次的状态
                        Integer previousState = countValueState.value();
                        //获取当前状态值，也就是word出现的次数
                        Integer currentState = value.f1;

                        if (previousState == null) {
                            //如果之前状态为空 立即更新状态
                            countValueState.update(currentState);
                            out.collect(value);
                        } else {
                            Integer latestState = previousState + currentState;
                            //累加获取最新状态
                            countValueState.update(latestState);
                            out.collect(Tuple2.of(value.f0,latestState));
                        }

                    }
                });

        resultStream.printToErr();
        env.execute(FlinkWordCountProcess.class.getSimpleName());
    }
}
