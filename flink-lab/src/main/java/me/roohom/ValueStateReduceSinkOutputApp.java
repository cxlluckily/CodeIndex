package me.roohom;

import me.roohom.operator.ValueFlatMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ValueStateReduceSinkOutputApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Long, String>> list = new ArrayList<>();
        Long i = 0L;
        while (true) {
            list.add(Tuple2.of(1L, i + "-x"));
            i++;
            if (i > 100) {
                break;
            }
        }

        DataStreamSource<Tuple2<Long, String>> inputStream = env.fromCollection(list);
        inputStream.keyBy(x -> x.f0)
                .flatMap(new ValueFlatMapFunction())
                .print();

        env.execute();

    }
}
