package me.roohom.operator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class ValueFlatMapFunction extends RichFlatMapFunction<Tuple2<Long, String>, String> {

    private transient ValueState<Long> reduceOut;
    private ArrayList<String> strList;

    @Override
    public void open(Configuration parameters) throws Exception {
        strList = new ArrayList<>();
        super.open(parameters);
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "reduceOutput", // the state name
                        TypeInformation.of(new TypeHint<Long>() {
                        }), // type information
                        0L); // default value of the state, if nothing was set
        reduceOut = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, String> value, Collector<String> out) throws Exception {
        // access the state value
        Long current = reduceOut.value();

        // update the count
        current += 1;
        strList.add(value.f1);

        // update the state
        reduceOut.update(current);
        if (current % 10 == 0) {
            out.collect(strList.toString());
            strList.clear();
        }
    }
}
